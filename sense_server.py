import asyncio
import websockets
import json
import time
import numpy as np
import sys

sys.path.append('SenseVoice')

# 避免在不同线程/协程中引发初始化问题，我们在启动时初始化
try:
    import webrtcvad
except ImportError:
    print("Please install webrtcvad: pip install webrtcvad")

from funasr import AutoModel
from funasr.utils.postprocess_utils import rich_transcription_postprocess

# ---------------------------------------------------------------------------
# ASR WebSocket 微服务服务端 (sense_server.py)
# 职责：持续接收音频流，基于 WebRTC VAD 切断，并调用 SenseVoice 获取流式结果
# ---------------------------------------------------------------------------

class StreamingASRServer:
    def __init__(self, model_dir="iic/SenseVoiceSmall", device="cuda:0"):
        print("Loading SenseVoice model...")
        self.model = AutoModel(
            model=model_dir,
            trust_remote_code=True,
            remote_code="SenseVoice/model.py",
            vad_model="fsmn-vad",
            vad_kwargs={"max_single_segment_time": 30000},
            device=device,
        )
        self.vad = webrtcvad.Vad(3)
        self.sample_rate = 16000
        self.frame_duration_ms = 30
        self.max_silence_frames = int(500 / self.frame_duration_ms)

    def do_inference(self, audio_bytes):
        if len(audio_bytes) < self.sample_rate * 0.1 * 2: # At least 100ms
            return ""
        audio_np = np.frombuffer(audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        try:
            res = self.model.generate(
                input=audio_np,
                cache={},
                language="auto",
                use_itn=True,
                batch_size_s=60,
                merge_vad=True,
                merge_length_s=15,
                disable_pbar=True
            )
            text = rich_transcription_postprocess(res[0]["text"])
            return text
        except Exception:
            return ""

asr_engine = None

async def asr_handler(websocket):
    remote_address = websocket.remote_address
    print(f"[ASR Server] 客户端已连接: {remote_address}")
    
    buffer = b""
    is_speaking = False
    last_infer_time = 0
    infer_interval = 0.3 # 300ms 伪流式
    silence_frames = 0
    
    global asr_engine
    
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                # We expect chunks of 30ms (960 bytes for 16000Hz 16-bit mono)
                try:
                    # WebRTC VAD expects exactly 10, 20 or 30ms frames
                    is_speech = asr_engine.vad.is_speech(message, asr_engine.sample_rate)
                except Exception as e:
                    # If frame size is wrong, assume speech to keep appending
                    is_speech = True
                    
                if is_speech:
                    silence_frames = 0
                    if not is_speaking:
                        is_speaking = True
                        buffer = b""
                        last_infer_time = time.time()
                    buffer += message
                else:
                    if is_speaking:
                        buffer += message
                        silence_frames += 1
                        
                        if silence_frames > asr_engine.max_silence_frames:
                            # 说话结束
                            is_speaking = False
                            text = asr_engine.do_inference(buffer)
                            reply = {"text": text, "is_final": True}
                            await websocket.send(json.dumps(reply))
                            buffer = b""
                            silence_frames = 0
                            continue
                            
                # 伪流式推理
                if is_speaking and (time.time() - last_infer_time) >= infer_interval:
                    text = asr_engine.do_inference(buffer)
                    if text:
                        reply = {"text": text, "is_final": False}
                        await websocket.send(json.dumps(reply))
                    last_infer_time = time.time()

    except websockets.ConnectionClosed:
        print(f"[ASR Server] 客户端断开连接: {remote_address}")
    except Exception as e:
        print(f"[ASR Server] 异常: {e}")

async def main():
    global asr_engine
    asr_engine = StreamingASRServer()
    # 增加 ws 协议中非常重要的 ping/pong 超时上限，防止因为模型调用造成的协程拥堵导致断连
    server = await websockets.serve(asr_handler, "127.0.0.1", 8000, ping_interval=None, ping_timeout=None)
    print("====================================")
    print(" ASR Microservice 启动成功")
    print(" 监听地址: ws://127.0.0.1:8000")
    print("====================================")
    await server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("ASR Server 已关闭")
