import Foundation
import Network

final class ZeehoOutput {
    static let width = 1024
    static let height = 600
    static let fps = 30

    private let queue = DispatchQueue(label: "CarWithin.tailgmax", qos: .userInitiated)
    private var session: Session?

    deinit { stop() }

    func start(
        localAddress: String,
        targetAddress: String,
        phone: TailingPhoneInfo,
        onState: @escaping (String) -> Void,
        onLog: @escaping (String) -> Void,
        requestKeyframe: @escaping () -> Void
    ) {
        queue.async { [weak self] in
            guard let self else { return }
            self.session?.stop()
            let newSession = Session(
                queue: self.queue,
                onState: onState,
                onLog: onLog,
                requestKeyframe: requestKeyframe
            )
            self.session = newSession
            newSession.start()
        }
    }

    func append(_ data: Data, codec: VideoCodec, keyframe: Bool) {
        guard codec == .h264 else { return }
        queue.async { [weak self] in
            self?.session?.append(data, keyframe: keyframe)
        }
    }

    func stop() {
        queue.sync {
            session?.stop()
            session = nil
        }
    }
}

private extension ZeehoOutput {
    final class Session {
        private let queue: DispatchQueue
        private let onState: (String) -> Void
        private let onLog: (String) -> Void
        private let requestKeyframe: () -> Void

        private var mainListener: NWListener?
        private var videoListener: NWListener?
        private var heartListener: NWListener?

        private var mainConn: NWConnection?
        private var videoConn: NWConnection?
        private var heartConn: NWConnection?

        private var mainHeartbeatTimer: DispatchSourceTimer?
        private var heartBeatTimer: DispatchSourceTimer?

        private var videoReady = false
        private var awaitingKeyframe = true
        private var stopped = false

        init(
            queue: DispatchQueue,
            onState: @escaping (String) -> Void,
            onLog: @escaping (String) -> Void,
            requestKeyframe: @escaping () -> Void
        ) {
            self.queue = queue
            self.onState = onState
            self.onLog = onLog
            self.requestKeyframe = requestKeyframe
        }

        func start() {
            do {
                mainListener = try makeListener(port: 17818) { [weak self] conn in
                    self?.handleMainConnection(conn)
                }
                videoListener = try makeListener(port: 15456) { [weak self] conn in
                    self?.handleVideoConnection(conn)
                }
                heartListener = try makeListener(port: 15457) { [weak self] conn in
                    self?.handleHeartConnection(conn)
                }
                onState("等待王者MAX仪表连入 (17818/15456/15457)…")
                onLog("[TailgMax] TCP 监听已启动: 17818, 15456, 15457")
            } catch {
                onState("端口监听失败: \(error.localizedDescription)")
                onLog("[TailgMax] 启动失败: \(error)")
            }
        }

        func append(_ data: Data, keyframe: Bool) {
            guard !stopped, videoReady, let videoConn else { return }
            if awaitingKeyframe {
                guard keyframe else { return }
                awaitingKeyframe = false
            }
            let annexB = Self.ensureAnnexB(data)
            videoConn.send(content: annexB, completion: .contentProcessed { [weak self] error in
                if let error {
                    self?.onLog("[TailgMax] 视频发送错误: \(error)")
                }
            })
        }

        func stop() {
            stopped = true
            videoReady = false
            mainHeartbeatTimer?.cancel()
            heartBeatTimer?.cancel()
            mainConn?.cancel()
            videoConn?.cancel()
            heartConn?.cancel()
            mainListener?.cancel()
            videoListener?.cancel()
            heartListener?.cancel()
        }

        private func makeListener(port: UInt16, onAccept: @escaping (NWConnection) -> Void) throws -> NWListener {
            let params = NWParameters.tcp
            params.allowLocalEndpointReuse = true
            let listener = try NWListener(using: params, on: NWEndpoint.Port(rawValue: port)!)
            listener.newConnectionHandler = onAccept
            listener.start(queue: queue)
            return listener
        }

        private func handleMainConnection(_ conn: NWConnection) {
            mainConn?.cancel()
            mainConn = conn
            conn.stateUpdateHandler = { [weak self] state in
                guard let self, !self.stopped else { return }
                if case .ready = state {
                    self.onLog("[TailgMax] 17818 主控已连接")
                    self.sendMainHandshake(on: conn)
                    self.startMainHeartbeat(on: conn)
                    self.readMainLoop(on: conn, buffer: Data())
                }
            }
            conn.start(queue: queue)
        }

        private func sendMainHandshake(on conn: NWConnection) {
            send(Self.jsonFrame(#"{"msg_id":27,"func":"TUC","act":"GET"}"#), on: conn)
            var devType = UInt32(2).bigEndian
            send(Self.binaryFrame(type: 0x01, cmd: 0x17, payload: Data(bytes: &devType, count: 4)), on: conn)
            var nick = Data(count: 256)
            let nameBytes = Array("CarWithin".utf8)
            for (i, b) in nameBytes.prefix(256).enumerated() { nick[i] = b }
            send(Self.binaryFrame(type: 0x01, cmd: 0x0C, payload: nick), on: conn)
        }

        private func startMainHeartbeat(on conn: NWConnection) {
            mainHeartbeatTimer?.cancel()
            let timer = DispatchSource.makeTimerSource(queue: queue)
            timer.schedule(deadline: .now() + 2.0, repeating: 2.0)
            timer.setEventHandler { [weak self] in
                guard let self, !self.stopped else { return }
                self.send(Self.binaryFrame(type: 0x02, cmd: 0x01), on: conn)
            }
            timer.resume()
            mainHeartbeatTimer = timer
        }

        private func readMainLoop(on conn: NWConnection, buffer: Data) {
            conn.receive(minimumIncompleteLength: 1, maximumLength: 8192) { [weak self] data, _, isComplete, error in
                guard let self, !self.stopped else { return }
                if let data, !data.isEmpty {
                    var buf = buffer
                    buf.append(data)
                    buf = self.processMainBuffer(buf, on: conn)
                    if !isComplete && error == nil {
                        self.readMainLoop(on: conn, buffer: buf)
                    }
                }
            }
        }

        private func processMainBuffer(_ input: Data, on conn: NWConnection) -> Data {
            var buf = input
            while buf.count >= 6 {
                let type = buf[buf.startIndex]
                let cmd = buf[buf.startIndex + 1]
                let len = (Int(buf[buf.startIndex + 2]) << 24) |
                          (Int(buf[buf.startIndex + 3]) << 16) |
                          (Int(buf[buf.startIndex + 4]) << 8) |
                          Int(buf[buf.startIndex + 5])
                let isJSON = (type == 0xEE && cmd == 0xFD)
                let totalLen = 6 + len + (isJSON ? 1 : 0)
                guard len >= 0, len <= 32 * 1024 * 1024, buf.count >= totalLen else { break }

                let payload = buf.subdata(in: (buf.startIndex + 6)..<(buf.startIndex + 6 + len))
                buf.removeFirst(totalLen)

                if isJSON, let text = String(data: payload, encoding: .utf8) {
                    if text.contains("TUC") && text.contains("GET") {
                        send(Self.jsonFrame(#"{"msg_id":27,"func":"TUC","act":"SEND","tuc":"","tucs":0}"#), on: conn)
                        send(Self.jsonFrame(#"{"msg_id":27,"func":"TUC","act":"STATUS","status":1}"#), on: conn)
                    }
                } else if type == 0x01 && len == 0 {
                    if cmd == 0x0F {
                        var shape = UInt32(7).bigEndian
                        send(Self.binaryFrame(type: 0x01, cmd: 0x0F, payload: Data(bytes: &shape, count: 4)), on: conn)
                    } else if cmd == 0x15 {
                        var lang = UInt32(0).bigEndian
                        send(Self.binaryFrame(type: 0x01, cmd: 0x15, payload: Data(bytes: &lang, count: 4)), on: conn)
                    }
                }
            }
            return buf
        }

        private func handleHeartConnection(_ conn: NWConnection) {
            heartConn?.cancel()
            heartConn = conn
            conn.stateUpdateHandler = { [weak self] state in
                guard let self, !self.stopped else { return }
                if case .ready = state {
                    self.onLog("[TailgMax] 15457 心跳已连接")
                    self.heartBeatTimer?.cancel()
                    let timer = DispatchSource.makeTimerSource(queue: self.queue)
                    timer.schedule(deadline: .now(), repeating: .milliseconds(450))
                    timer.setEventHandler { [weak self] in
                        guard let self, !self.stopped else { return }
                        self.send(Data([0x02, 0x01, 0x00, 0x00, 0x00, 0x00]), on: conn)
                    }
                    timer.resume()
                    self.heartBeatTimer = timer
                    self.drainLoop(on: conn)
                }
            }
            conn.start(queue: queue)
        }

        private func drainLoop(on conn: NWConnection) {
            conn.receive(minimumIncompleteLength: 1, maximumLength: 256) { [weak self] _, _, isComplete, error in
                guard let self, !self.stopped, !isComplete, error == nil else { return }
                self.drainLoop(on: conn)
            }
        }

        private func handleVideoConnection(_ conn: NWConnection) {
            videoConn?.cancel()
            videoConn = conn
            videoReady = false
            awaitingKeyframe = true
            conn.stateUpdateHandler = { [weak self] state in
                guard let self, !self.stopped else { return }
                if case .ready = state {
                    let header = Self.videoHeader(width: UInt16(ZeehoOutput.width), height: UInt16(ZeehoOutput.height))
                    self.send(header, on: conn)
                    self.videoReady = true
                    self.onState("正在投屏 · 王者MAX 1024×600 · H.264 · 30 FPS")
                    self.onLog("[TailgMax] 15456 视频通道已就绪，已发送 69 字节头")
                    self.requestKeyframe()
                }
            }
            conn.start(queue: queue)
        }

        private func send(_ data: Data, on conn: NWConnection) {
            conn.send(content: data, completion: .contentProcessed { _ in })
        }

        private static func binaryFrame(type: UInt8, cmd: UInt8, payload: Data = Data()) -> Data {
            var out = Data(capacity: 6 + payload.count)
            out.append(type)
            out.append(cmd)
            var lenBE = UInt32(payload.count).bigEndian
            out.append(Data(bytes: &lenBE, count: 4))
            out.append(payload)
            return out
        }

        private static func jsonFrame(_ json: String) -> Data {
            let payload = Data(json.utf8)
            var out = Data(capacity: 6 + payload.count + 1)
            out.append(0xEE)
            out.append(0xFD)
            var lenBE = UInt32(payload.count).bigEndian
            out.append(Data(bytes: &lenBE, count: 4))
            out.append(payload)
            out.append(0xFF)
            return out
        }

        private static func videoHeader(width: UInt16, height: UInt16) -> Data {
            var data = Data(count: 69)
            data[0] = 0x00
            for (i, b) in Array("android".utf8).enumerated() { data[1 + i] = b }
            data[65] = UInt8((width >> 8) & 0xFF)
            data[66] = UInt8(width & 0xFF)
            data[67] = UInt8((height >> 8) & 0xFF)
            data[68] = UInt8(height & 0xFF)
            return data
        }

        private static func ensureAnnexB(_ data: Data) -> Data {
            guard data.count >= 4 else { return data }
            if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 1) ||
               (data[0] == 0 && data[1] == 0 && data[2] == 1) {
                return data
            }
            var output = Data(capacity: data.count + 16)
            let startCode = Data([0x00, 0x00, 0x00, 0x01])
            var offset = 0
            while offset + 4 <= data.count {
                let naluLen = (Int(data[offset]) << 24) |
                              (Int(data[offset + 1]) << 16) |
                              (Int(data[offset + 2]) << 8) |
                              Int(data[offset + 3])
                offset += 4
                guard naluLen > 0, offset + naluLen <= data.count else { return data }
                output.append(startCode)
                output.append(data[offset..<(offset + naluLen)])
                offset += naluLen
            }
            return output.isEmpty ? data : output
        }
    }
}
