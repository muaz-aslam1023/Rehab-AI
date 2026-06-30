from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from routes.auth_routes import auth_routes
from routes.data_routes import data_routes
from routes.doctor_routes import doctor_routes
from routes.ml_routes import ml_routes
from routes.calories_routes import router as calories_router
from contextlib import asynccontextmanager
import os
import sys
from token_manager import token_manager


def ensure_ssl_certs():
    """Auto-generate self-signed SSL certs if they don't exist (needed for TCP TLS demo)."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cert_dir = os.path.join(current_dir, "certs")
    cert_path = os.path.join(cert_dir, "server.crt")
    key_path = os.path.join(cert_dir, "server.key")

    if os.path.exists(cert_path) and os.path.exists(key_path):
        return cert_path, key_path

    os.makedirs(cert_dir, exist_ok=True)
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from datetime import datetime, timedelta
        import ipaddress

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
        ])
        cert = (x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.utcnow())
            .not_valid_after(datetime.utcnow() + timedelta(days=3650))
            .add_extension(x509.SubjectAlternativeName([
                x509.DNSName(u"localhost"),
                x509.IPAddress(ipaddress.IPv4Address('127.0.0.1'))
            ]), critical=False)
            .sign(key, hashes.SHA256()))

        with open(key_path, "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption()
            ))
        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        print(f"✅ Auto-generated SSL certs in {cert_dir}")
    except Exception as e:
        print(f"⚠️ Could not generate SSL certs: {e}")

    return cert_path, key_path


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure SSL certs exist for TCP TLS server
    ensure_ssl_certs()

    try:
        # Start TCP Server in background
        tcp_task = asyncio.create_task(start_tcp_server())

        yield

        # Shutdown
        tcp_task.cancel()
        try:
            await tcp_task
        except asyncio.CancelledError:
            pass

    finally:
        pass


app = FastAPI(lifespan=lifespan)

# CORS — read allowed origins from env var (comma-separated), fallback to localhost for dev
_raw_origins = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173"
)
origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/api/validate_stream_token/{token}")
async def validate_stream_token(token: str):
    """Internal endpoint for ML server to validate tokens"""
    token_data = token_manager.validate_token(token)
    if token_data:
        return {"valid": True, "data": token_data}
    return {"valid": False}


# --- NATIVE TCP RECEIVER (Socket Programming) ---
from tcp_control_flow.socket_wrapper import SocketWrapper
from tcp_control_flow import ReliableReceiver
import asyncio
import json

TCP_PORT = 65432


async def handle_tcp_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"[TCP Server] Connection from {addr}")

    sock_wrapper = SocketWrapper(reader, writer)

    async def send_ack(ack_num, flags=[]):
        from tcp_control_flow.protocol import TCPPacket, FLAG_ACK
        ack_pkt = TCPPacket(seq_num=0, ack_num=ack_num, flags=[FLAG_ACK] + flags)
        await sock_wrapper.send(ack_pkt.to_json())

    receiver = ReliableReceiver(send_ack_callback=send_ack)

    try:
        while not receiver.is_finished:
            data_str = await sock_wrapper.recv()
            if not data_str:
                break

            await receiver.process_packet(data_str)

            if receiver.is_finished:
                full_data_bytes = receiver.get_reassembled_data()
                print(f"[TCP Server] Transfer Complete. Reassembled {len(full_data_bytes)} bytes.")

                db_success = False
                try:
                    report_json = json.loads(full_data_bytes.decode('utf-8'))

                    import db_connection

                    patient_id = report_json.get("patient_id")
                    exercise_id = report_json.get("exercise_id")
                    pain_data = report_json.get("pain_feedback")
                    session_stats = report_json.get("session_stats")

                    if patient_id and exercise_id:
                        success = db_connection.complete_exercise(patient_id, exercise_id, pain_data, session_stats)
                        if success:
                            db_success = True

                    print("✅ Data successfully passed to Data Layer for Storage.")

                except Exception as e:
                    print(f"[TCP Server] Error decoding or saving reassembled data: {e}")

                try:
                    status_msg = json.dumps({"type": "DB_STATUS", "success": db_success})
                    await sock_wrapper.send(status_msg)
                    print(f"[TCP Server] Sent DB Status: {db_success}")
                except Exception as e:
                    print(f"[TCP Server] Error sending confirmation: {e}")

                break

    except Exception as e:
        print(f"[TCP Server] Error handling client: {e}")
    finally:
        print(f"[TCP Server] Closing connection from {addr}")
        await sock_wrapper.close()


async def start_tcp_server():
    import ssl

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cert_path = os.path.join(current_dir, "certs", "server.crt")
    key_path = os.path.join(current_dir, "certs", "server.key")

    try:
        ssl_context.load_cert_chain(certfile=cert_path, keyfile=key_path)
        print(f"🔐 Loaded SSL Certs from {cert_path}")
    except Exception as e:
        print(f"❌ Failed to load SSL Certs: {e}")
        return

    server = await asyncio.start_server(
        handle_tcp_client, '127.0.0.1', TCP_PORT, ssl=ssl_context)

    addr = server.sockets[0].getsockname()
    print(f"🚀 TCP Socket Server (TLS) listening on {addr}")

    async with server:
        await server.serve_forever()


# --- TCP RELIABLE RECEIVER ENDPOINT (WebSocket fallback) ---
from fastapi import WebSocket, WebSocketDisconnect

@app.websocket("/api/data_transport_socket")
async def data_transport_socket(websocket: WebSocket):
    await websocket.accept()
    print("[Main] Transport Socket Connected")

    async def send_ack(ack_num, flags=[]):
        from tcp_control_flow.protocol import TCPPacket, FLAG_ACK
        pkt = TCPPacket(seq_num=0, ack_num=ack_num, flags=[FLAG_ACK] + flags)
        await websocket.send_text(pkt.to_json())

    receiver = ReliableReceiver(send_ack)

    try:
        while True:
            data = await websocket.receive_text()
            await receiver.process_packet(data)

            if receiver.is_finished:
                full_data_bytes = receiver.get_reassembled_data()

                try:
                    report_json = json.loads(full_data_bytes.decode('utf-8'))

                    import db_connection

                    patient_id = report_json.get("patient_id")
                    exercise_id = report_json.get("exercise_id")
                    pain_data = report_json.get("pain_feedback")
                    session_stats = report_json.get("session_stats")

                    if patient_id and exercise_id:
                        db_connection.complete_exercise(patient_id, exercise_id, pain_data, session_stats)

                except Exception as e:
                    print(f"[Main] Error decoding or saving reassembled data: {e}")

                break

    except Exception as e:
        print(f"[Main] Transport Error: {e}")


# --- ML POSE ANALYSIS WEBSOCKET (merged from ml/ml_server.py) ---
# Add ml directory to path so PoseAnalyzer and its deps can be imported
_ml_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ml")
if _ml_dir not in sys.path:
    sys.path.insert(0, _ml_dir)

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server environment

try:
    from pose_analysis import PoseAnalyzer
    _ML_AVAILABLE = True
    print("✅ ML PoseAnalyzer loaded successfully")
except Exception as e:
    _ML_AVAILABLE = False
    print(f"⚠️ ML PoseAnalyzer not available: {e}")


@app.websocket("/ws/pose-analysis/{token}")
async def websocket_pose_analysis(websocket: WebSocket, token: str):
    """Real-time pose analysis WebSocket (merged from ml_server.py)."""
    await websocket.accept()

    if not _ML_AVAILABLE:
        await websocket.close(code=1011, reason="ML service unavailable")
        return

    # Validate token directly via token_manager (no HTTP call needed)
    token_data = token_manager.validate_token(token)
    if not token_data:
        await websocket.close(code=4003, reason="Invalid or expired token")
        return

    current_patient_id = token_data.get("patient_id")
    current_exercise_id = token_data.get("exercise_id")

    pose_analyzer = PoseAnalyzer()
    pose_analyzer.reset_session()

    from datetime import datetime
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_id = f"{current_patient_id}_{current_exercise_id}_{timestamp_str}"
    print(f"[ML WS] Starting session: {session_id}")

    try:
        while True:
            data = await websocket.receive_text()

            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                continue

            if isinstance(payload, dict) and payload.get("command") == "complete_session":
                print(f"[ML WS] Session complete for {session_id}. Generating report...")
                pain_data = payload.get("pain_data")
                report = pose_analyzer.get_session_summary(pain_data)

                if current_patient_id and current_exercise_id:
                    report["patient_id"] = current_patient_id
                    report["exercise_id"] = current_exercise_id

                # Send data to TCP server (same machine, TLS)
                try:
                    from tcp_control_flow import ReliableSender, TCPPacket
                    import ssl as _ssl

                    ssl_ctx = _ssl.create_default_context(_ssl.Purpose.SERVER_AUTH)
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = _ssl.CERT_NONE

                    reader, writer = await asyncio.open_connection('127.0.0.1', TCP_PORT, ssl=ssl_ctx)
                    sock_wrapper = SocketWrapper(reader, writer)

                    data_bytes = json.dumps(report).encode('utf-8')
                    sender = ReliableSender(sock_wrapper, data_bytes, session_id=session_id)

                    async def listen_acks():
                        try:
                            while True:
                                msg_str = await sock_wrapper.recv()
                                if not msg_str:
                                    break
                                pkt = TCPPacket.from_json(msg_str)
                                if pkt:
                                    sender.process_ack(pkt.ack_num)
                        except Exception:
                            pass

                    ack_task = asyncio.create_task(listen_acks())
                    await sender.handshake()
                    await asyncio.sleep(0.1)
                    await sender.send_data()

                    if not ack_task.done():
                        ack_task.cancel()

                    db_confirmed = False
                    try:
                        import time
                        start_wait = time.time()
                        while time.time() - start_wait < 5.0:
                            try:
                                resp_str = await asyncio.wait_for(sock_wrapper.recv(), timeout=1.0)
                                if not resp_str:
                                    break
                                resp_json = json.loads(resp_str)
                                if resp_json.get("type") == "DB_STATUS":
                                    db_confirmed = resp_json.get("success", False)
                                    break
                                if any(k in resp_json for k in ("seq", "ack", "flags")):
                                    continue
                            except asyncio.TimeoutError:
                                continue
                    except Exception:
                        pass

                    await sock_wrapper.close()

                    try:
                        from network_sim.network_stats_plotter import plot_tcp_stats
                        stats = sender.get_transfer_stats()
                        plot_tcp_stats(stats, session_id=session_id)
                    except Exception:
                        pass

                    if db_confirmed:
                        await websocket.send_json({
                            "status": "completed",
                            "redirect": True,
                            "message": "Session saved successfully!"
                        })

                except Exception as e:
                    print(f"[ML WS] TCP transport error: {e}")

                break

            # Normal frame — process landmarks
            metrics = pose_analyzer.process_frame_landmarks(payload)
            await websocket.send_json(metrics)
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        print(f"[ML WS] Client disconnected: {session_id}")
    except Exception as e:
        print(f"[ML WS] Error: {e}")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


# Register routers
app.include_router(auth_routes)
app.include_router(data_routes)
app.include_router(doctor_routes)
app.include_router(calories_router)
app.include_router(ml_routes)
