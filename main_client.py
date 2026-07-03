import threading
import queue
import time
import sys
import json
import traceback
import pyaudio
import dashscope
from http import HTTPStatus

from websocket import create_connection, WebSocketConnectionClosedException

import argparse

# 配置通义千问 API 鉴权，建议使用环境变量
dashscope.api_key = 'sk-190abd83d41b428b845842131baa952b'

class VoiceInteractionClient:
    def __init__(self, use_microphone=True, file_input=None):
        self.use_microphone = use_microphone
        self.file_input = file_input
        self.Text_Prompt_Queue = queue.Queue()       
        self.Sentence_Queue = queue.Queue()          
        self.Audio_Chunk_Queue = queue.Queue()       

        self.running = False
        self.split_punctuation = set(['。', '！', '？', '；', '.', '!', '?', ';', '\n'])
        
        # 打断机制相关状态
        self.is_interrupted = False
        self.stop_words = ["停", "停下", "停止", "闭嘴", "等等", "打住", "好了", "行了", "可以了", "别说了", "换个话题", "换一个话题", "不要说了"]

        # 多轮对话历史状态管理
        self.chat_history = [{'role': 'system', 'content': '你是一个有用的语音对话助理，请用简短、自然、口语化的语言与用户进行连续对话。'}]
        self.max_history_turns = 10 # 限制保存的最长历史轮数

        self.ASR_WS_URL = "ws://127.0.0.1:8000"
        self.TTS_WS_URL = "ws://127.0.0.1:8001"
        
        # 全局初始化 PyAudio，防止多线程同时初始化导致 ALSA/PortAudio 底层死锁与 Core Dump
        try:
            self.pyaudio_instance = pyaudio.PyAudio()
        except Exception as e:
            print(f"[Warning] 全局 PyAudio 初始化失败: {e}")
            self.pyaudio_instance = None

    def start(self):
        self.running = True
        print("[System] 正在启动分布式全链路语音总管中控台...")
        
        self.thread_asr = threading.Thread(target=self._thread_asr_worker, name="Thread_ASR", daemon=True)
        self.thread_llm = threading.Thread(target=self._thread_llm_worker, name="Thread_LLM", daemon=True)
        self.thread_tts = threading.Thread(target=self._thread_tts_worker, name="Thread_TTS", daemon=True)
        self.thread_player = threading.Thread(target=self._thread_player_worker, name="Thread_Player", daemon=True)

        self.thread_asr.start()
        self.thread_llm.start()
        self.thread_tts.start()
        self.thread_player.start()
        
        print("[System] 4个微服务分发线程已启动。按下 Ctrl+C 终止并退出。\n")

    def stop(self):
        self.running = False
        self.Text_Prompt_Queue.put(None)
        self.Sentence_Queue.put(None)
        self.Audio_Chunk_Queue.put(None)

    def interrupt(self):
        self.is_interrupted = True
        # 清空所有队列
        for q in [self.Text_Prompt_Queue, self.Sentence_Queue, self.Audio_Chunk_Queue]:
            while not q.empty():
                try:
                    q.get_nowait()
                except queue.Empty:
                    break
        print("\n\n[System] ---------------- 收到打断指令，已切断播报与推理任务 ----------------\n")

    def _thread_asr_worker(self):
        ws = None
        while self.running:
            try:
                if ws is None or not ws.connected:
                    # 移除超时时间，防止 ASR 阻塞耗时超过 3 秒导致的 recv 线程自动断开
                    ws = create_connection(self.ASR_WS_URL)
                    
                # 定义接收线程，以防麦克风读取阻塞网络回传
                def recv_loop(ws_conn):
                    while self.running and ws_conn and ws_conn.connected:
                        try:
                            res_str = ws_conn.recv()
                            res = json.loads(res_str)
                            current_text = res.get("text", "")
                            
                            sys.stdout.write(f"\r[You]: {current_text}\033[K")
                            sys.stdout.flush()
                            
                            if res.get("is_final", False):
                                sys.stdout.write(f"\r[You]: {current_text}\n")
                                
                                clean_text = current_text.strip('。！？，.,!? \n')
                                if not clean_text:
                                    continue
                                
                                # 判断是否是打断词
                                is_break = False
                                for word in self.stop_words:
                                    if clean_text == word or clean_text.startswith(word) or (len(word) >= 3 and word in clean_text):
                                        is_break = True
                                        break
                                
                                if is_break:
                                    self.interrupt()
                                else:
                                    self.is_interrupted = False
                                    print(f"[Debug] 识别到最终结果，正在送入大模型队列... : {current_text}")
                                    self.Text_Prompt_Queue.put(current_text)
                        except Exception as recv_e:
                            if "timed out" in str(recv_e).lower() or "timeout" in str(recv_e).lower():
                                continue
                            print(f"\n[Thread_ASR] ASR 下行接收线程断开: {recv_e}")
                            break

                recv_thread = threading.Thread(target=recv_loop, args=(ws,), daemon=True)
                recv_thread.start()

                if self.use_microphone:
                    # 尝试打开 PyAudio 麦克风
                    try:
                        stream = self.pyaudio_instance.open(format=pyaudio.paInt16,
                                        channels=1,
                                        rate=16000,
                                        input=True,
                                        frames_per_buffer=1024)
                        print("\n[麦克风已开启，开始录音，随时说话...]")
                        
                        while self.running and ws.connected:
                            # 严格以 480 帧 (30ms / 960字节) 进行数据采集
                            # 这是由于服务端的 WebRTC VAD 引擎严格限制仅支持 10、20、30ms 长度检测
                            data = stream.read(480, exception_on_overflow=False)
                            ws.send_binary(data)
                    except Exception as audio_e:
                        print(f"\n[Thread_ASR] 异常中断或麦克风故障 ({audio_e})")
                        time.sleep(2)
                else:
                    # 临时使用 指定的文件 作为音频输入
                    try:
                        import wave
                        print(f"\n[测试模式: 正在循环读取本地音频 {self.file_input}...]")
                        wf = wave.open(self.file_input, 'rb')
                        while self.running and ws.connected:
                            data = wf.readframes(480) # 480 frames * 2 bytes = 960 bytes = 30ms
                            if not data:
                                wf.rewind() # 循环播放
                                time.sleep(1)
                                continue
                            ws.send_binary(data)
                            time.sleep(0.03) # 严格控制推流速度为 30ms
                    except Exception as audio_e:
                        print(f"\n[音频模拟报错]: {audio_e}")
                        time.sleep(2)
                    
            except Exception as e:
                sys.stdout.write(f"\r[Thread_ASR] ASR 服务未命中/已断开，重试中... ({e})\033[K")
                sys.stdout.flush()
                if ws: ws.close()
                ws = None
                time.sleep(2)
        if ws:
            ws.close()

    def _thread_llm_worker(self):
        try:
            while self.running:
                prompt = self.Text_Prompt_Queue.get()
                if prompt is None:
                    break
                    
                print("[LLM] 收到提问，开始流式推理...")
                char_generator = self._real_llm_stream(prompt)
                
                sentence_buffer = ""
                for char in char_generator:
                    if not self.running or self.is_interrupted: 
                        break
                    sys.stdout.write(char)
                    sys.stdout.flush()
                    sentence_buffer += char

                    if char in self.split_punctuation:
                        sentence = sentence_buffer.strip()
                        if sentence:
                            self.Sentence_Queue.put(sentence)
                        sentence_buffer = ""
                        
                print() 
                if sentence_buffer.strip():
                    self.Sentence_Queue.put(sentence_buffer.strip())
                    
        except Exception as e:
            print(f"\n[Thread_LLM] 发生异常: {e}")

    def _thread_tts_worker(self):
        ws = None
        while self.running:
            try:
                sentence = self.Sentence_Queue.get()
                if sentence is None:
                    break
                    
                if ws is None or not ws.connected:
                    ws = create_connection(self.TTS_WS_URL, timeout=120)
                
                ws.send(sentence.encode('utf-8'))
                
                while self.running:
                    if self.is_interrupted:
                        if ws: ws.close()
                        ws = None
                        break
                    
                    try:
                        chunk = ws.recv()
                        if chunk == b"END_OF_AUDIO":
                            break
                        self.Audio_Chunk_Queue.put(chunk)
                    except Exception as e:
                        if "timed out" in str(e).lower() or "timeout" in str(e).lower():
                            continue
                        raise e

            except Exception as e:
                print(f"\n[Thread_TTS] TTS 连接异常，重新建联 ({e})")
                if ws: ws.close()
                ws = None
                time.sleep(2)
        if ws:
            ws.close()

    def _thread_player_worker(self):
        print("[Player] 尝试开启硬件播放输出...")
        try:
            # 根据 CosyVoice 的格式设置：22050 采样率，Float32 格式
            stream = self.pyaudio_instance.open(format=pyaudio.paFloat32,
                            channels=1,
                            rate=22050,
                            output=True)
            print("[Player] 音频播放硬件已就绪。")
            
            while self.running:
                audio_chunk = self.Audio_Chunk_Queue.get()
                if audio_chunk is None:
                    break
                stream.write(audio_chunk)
                
        except Exception as e:
            print(f"\n[Thread_Player] 无法初始化硬件播放器，音频将被直接丢弃静音处理: {e}")
            while self.running:
                audio_chunk = self.Audio_Chunk_Queue.get()
                if audio_chunk is None:
                    break
                # 做无声消耗
                time.sleep(0.01)

    def _real_llm_stream(self, prompt):
        # 记录用户的多轮提问
        self.chat_history.append({'role': 'user', 'content': prompt})
        
        # 截断历史，防止超长上下文（始终保留第0个System Prompt）
        if len(self.chat_history) > self.max_history_turns * 2 + 1:
            self.chat_history = [self.chat_history[0]] + self.chat_history[-(self.max_history_turns * 2):]
        
        responses = dashscope.Generation.call(
            model='qwen-turbo',
            messages=self.chat_history,
            result_format='message',
            stream=True,
            incremental_output=True # 这参数开启增量(delta)返回
        )
        
        full_response_buffer = ""
        for response in responses:
            if self.is_interrupted:
                break
            if response.status_code == HTTPStatus.OK:
                delta = response.output.choices[0].message.content
                full_response_buffer += delta
                for char in delta:
                    yield char
            else:
                print(f"[LLM Error] Status: {response.status_code}, Msg: {response.message}")
                
        # 将大模型的完整回复保存进历史（若被用户打断，则保留被打断前的半截回复，保持语境真实连贯）
        if full_response_buffer.strip():
            self.chat_history.append({'role': 'assistant', 'content': full_response_buffer})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Streaming Voice Interaction System")
    parser.add_argument("--input", type=str, default="mic", help="Input source: 'mic' or path to a .wav file")
    args = parser.parse_args()

    # 解析输入方式
    if args.input == "mic":
        system = VoiceInteractionClient(use_microphone=True)
    else:
        system = VoiceInteractionClient(use_microphone=False, file_input=args.input)

    try:
        system.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n收到退出信号，终止程序执行。")
        system.stop()
        sys.exit(0)
