import asyncio
import websockets
import sys
import os
import json

# 添加 CosyVoice 及其 Matcha-TTS 依赖路径（适配上层目录运行）
sys.path.append('CosyVoice')
sys.path.append('CosyVoice/third_party/Matcha-TTS')
from cosyvoice.cli.cosyvoice import CosyVoice2

print("[TTS Server] 正在全局加载 CosyVoice2 模型，请稍候...")
cosyvoice = CosyVoice2(
    'CosyVoice/pretrained_models/CosyVoice2-0.5B', 
    load_jit=False, 
    load_trt=False, 
    fp16=False
)
prompt_wav_path = 'CosyVoice/asset/zero_shot_prompt.wav'
prompt_text = '希望你以后能够做的比我还好呦。'
print("[TTS Server] CosyVoice2 模型加载完成！")

async def tts_handler(websocket):
    remote_address = websocket.remote_address
    print(f"[TTS Server] 客户端已连接: {remote_address}")
    
    try:
        async for message in websocket:
            print(f"[TTS Server] 收到要合成的文本: {message}")
            
            # 调用 CosyVoice 流式接口
            for i, result in enumerate(cosyvoice.inference_zero_shot(message, prompt_text, prompt_wav_path, stream=True)):
                tts_speech_tensor = result['tts_speech']
                # 将 Pytorch Tensor 转换为 NumPy 并截取二进制 Bytes (格式为 Float32)
                audio_bytes = tts_speech_tensor.cpu().numpy().tobytes()
                await websocket.send(audio_bytes)
                # 微小让步，防止阻塞事件循环
                await asyncio.sleep(0.001)
                
            await websocket.send(b"END_OF_AUDIO")
            print("[TTS Server] 当前句子音频流下发完毕")

    except websockets.ConnectionClosed:
        print(f"[TTS Server] 客户端断开连接: {remote_address}")
    except Exception as e:
        print(f"[TTS Server] 异常: {e}")

async def main():
    server = await websockets.serve(tts_handler, "127.0.0.1", 8001, ping_interval=None, ping_timeout=None)
    print("====================================")
    print(" TTS Microservice 启动成功")
    print(" 监听地址: ws://127.0.0.1:8001")
    print("====================================")
    await server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("TTS Server 已关闭")
