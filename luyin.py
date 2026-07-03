import pyaudio
import wave

# 音频参数配置（大模型最爱的标准格式）
CHUNK = 1024
FORMAT = pyaudio.paInt16  # 16-bit
CHANNELS = 1              # 单声道
RATE = 16000              # 16kHz 采样率
RECORD_SECONDS = 8        # 录制 5 秒
WAVE_OUTPUT_FILENAME = "test_audio.wav"

p = pyaudio.PyAudio()

print("🔴 开始录音，请说话...")
stream = p.open(format=FORMAT, channels=CHANNELS,
                rate=RATE, input=True, frames_per_buffer=CHUNK)

frames = []
# 循环读取麦克风数据
for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
    data = stream.read(CHUNK)
    frames.append(data)

print("⏹ 录音结束！")
stream.stop_stream()
stream.close()
p.terminate()

# 保存为 wav 文件
wf = wave.open(WAVE_OUTPUT_FILENAME, 'wb')
wf.setnchannels(CHANNELS)
wf.setsampwidth(p.get_sample_size(FORMAT))
wf.setframerate(RATE)
wf.writeframes(b''.join(frames))
wf.close()