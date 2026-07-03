# import threading
# import queue
# import time
# import sys
# import re
# import traceback

# # ---------------------------------------------------------------------------
# # 全链路流式低延迟语音交互系统 - 中控台核心架构
# # 包含 4 线程 (ASR, LLM, TTS, Player) 与 3 队列，压榨端到端交互延迟至极低
# # ---------------------------------------------------------------------------

# class VoiceInteractionSystem:
#     def __init__(self):
#         # 【架构设计：3 个关键队列】
#         # 1. 存放 ASR 识别出的完整用户句子（字符串）
#         self.Text_Prompt_Queue = queue.Queue()
        
#         # 2. 存放 LLM 输出的切分好的短句（字符串），准备送去合成
#         self.Sentence_Queue = queue.Queue()
        
#         # 3. 存放 TTS 合成出的音频二进制碎片（bytes），准备播放
#         self.Audio_Chunk_Queue = queue.Queue()

#         # 线程控制标志
#         self.running = False
        
#         # 定义断句标点，遇到这些标点即视为一句结束，立刻送入 TTS
#         self.split_punctuation = set(['。', '！', '？', '；', '.', '!', '?', ';', '\n'])

#     def start(self):
#         self.running = True
#         print("[System] 正在启动全链路流式语音交互系统...")
        
#         # 【架构设计：4 个独立工作线程】
#         self.thread_asr = threading.Thread(target=self._thread_asr_worker, name="Thread_ASR", daemon=True)
#         self.thread_llm = threading.Thread(target=self._thread_llm_worker, name="Thread_LLM", daemon=True)
#         self.thread_tts = threading.Thread(target=self._thread_tts_worker, name="Thread_TTS", daemon=True)
#         self.thread_player = threading.Thread(target=self._thread_player_worker, name="Thread_Player", daemon=True)

#         self.thread_asr.start()
#         self.thread_llm.start()
#         self.thread_tts.start()
#         self.thread_player.start()
        
#         print("[System] 4个核心工作线程已启动。按下 Ctrl+C 终止系统。\n")

#     def stop(self):
#         print("[System] 正在关闭系统...")
#         self.running = False
#         # 放入结束标记打断阻塞
#         self.Text_Prompt_Queue.put(None)
#         self.Sentence_Queue.put(None)
#         self.Audio_Chunk_Queue.put(None)

#     # =========================================================================
#     # 线程1: ASR 工作线程 (模拟麦克风采集、VAD 和流式识别)
#     # =========================================================================
#     def _thread_asr_worker(self):
#         """
#         线程指责：麦克风录音 + VAD切分 + 流式ASR请求。
#         输出：终端打印中间结果覆盖，最终结果推入 Text_Prompt_Queue。
#         """
#         try:
#             # 真实场景中，这里应使用 pyaudio 阻塞/回调读取麦克风数据
#             # 并且内嵌 VAD 检测，遇到静音(400ms~600ms)立刻截断
#             while self.running:
#                 # 模拟用户说了一句话，并通过 ASR 流式返回
#                 # 极简时延优化点：VAD截断要快！一旦确认用户停顿超过 500ms 即触发 is_final
#                 prompt_generator = self._mock_asr_stream()
                
#                 final_text = ""
#                 for result in prompt_generator:
#                     if not self.running:
#                         break
                        
#                     # 动态覆盖当前行，实现流式中间结果刷新
#                     current_text = result["text"]
#                     sys.stdout.write(f"\r[You]: {current_text}\033[K")
#                     sys.stdout.flush()
                    
#                     if result.get("is_final", False):
#                         final_text = current_text
#                         sys.stdout.write(f"\r[You]: {final_text}\n") # 换行固定
#                         sys.stdout.flush()
#                         break
                        
#                 if final_text:
#                     # 丢入后续管线
#                     self.Text_Prompt_Queue.put(final_text)
                    
#                 time.sleep(3) # 模拟用户发呆，3秒后说下一句
                
#         except Exception as e:
#             print(f"\n[Thread_ASR] 发生异常: {e}")
#             traceback.print_exc()

#     # =========================================================================
#     # 线程2: LLM 工作线程 (阻塞等待 Prompt，流式对话，基于标点断句)
#     # =========================================================================
#     def _thread_llm_worker(self):
#         """
#         线程指责：请求大模型产生流式文字，使用标点符号进行【文本缓冲切分】。
#         输出：拼装好的短句推入 Sentence_Queue 送往 TTS 线程。
#         """
#         try:
#             while self.running:
#                 prompt = self.Text_Prompt_Queue.get()
#                 if prompt is None: # 收到结束信号
#                     break
                    
#                 print(f"[LLM] 收到输入，开始思考并流式返回...")
#                 char_generator = self._mock_llm_stream(prompt)
                
#                 sentence_buffer = ""
#                 for char in char_generator:
#                     if not self.running:
#                         break
                        
#                     # 打印 LLM 本层级的输出
#                     sys.stdout.write(char)
#                     sys.stdout.flush()
                    
#                     sentence_buffer += char
                    
#                     # 极简时延优化点：利用标点断句（缓冲1）。
#                     # 一旦满一句，不用等整个 LLM 回复完，马上推进队列交给发音线程！
#                     if char in self.split_punctuation:
#                         sentence = sentence_buffer.strip()
#                         if sentence:
#                             self.Sentence_Queue.put(sentence)
#                         sentence_buffer = ""
                        
#                 print() # LLM 一次生成完全结束后换行
                
#                 # 处理未以标点结尾的残余文字
#                 if sentence_buffer.strip():
#                     self.Sentence_Queue.put(sentence_buffer.strip())
                    
#                 # 【信号传递】：通知 TTS 当前这一大段回复已发完，可以重置语境或处理结束符
#                 self.Sentence_Queue.put("<END_OF_TURN>")
                
#         except Exception as e:
#             print(f"\n[Thread_LLM] 发生异常: {e}")
#             traceback.print_exc()

#     # =========================================================================
#     # 线程3: TTS 工作线程 (流式语音合成，音频块双缓冲)
#     # =========================================================================
#     def _thread_tts_worker(self):
#         """
#         线程指责：从 Sentence_Queue 获取短句，送入流式 TTS (如 CosyVoice bistream)。
#         输出：音频块推入 Audio_Chunk_Queue。
#         """
#         try:
#             while self.running:
#                 sentence = self.Sentence_Queue.get()
#                 if sentence is None:
#                     break
                
#                 if sentence == "<END_OF_TURN>":
#                     # 当前回答的结尾，可在此向播放器发出停顿或截断信号
#                     continue
                    
#                 # print(f"[TTS] 开始合成短句: {sentence}")
                
#                 # 极简时延优化点：首句流式（缓冲2）。
#                 # TTS 一边合成，一边 yield 产生一小块一小块的音频二进制数据
#                 audio_chunk_generator = self._mock_tts_stream(sentence)
                
#                 for chunk in audio_chunk_generator:
#                     if not self.running:
#                         break
#                     # 合成出一块，立刻扔给播放器，不用等整句全完毕！大大降低首字响应延迟！
#                     self.Audio_Chunk_Queue.put(chunk)
                    
#         except Exception as e:
#             print(f"\n[Thread_TTS] 发生异常: {e}")
#             traceback.print_exc()

#     # =========================================================================
#     # 线程4: 播放 工作线程 (持续取音频块流式播放)
#     # =========================================================================
#     def _thread_player_worker(self):
#         """
#         线程指责：从 Audio_Chunk_Queue 提取二进制音频数据片，送给系统喇叭播放。
#         """
#         try:
#             # 真实场景中，这里会执行 stream = pyaudio.PyAudio().open(...)
#             print("[Player] 音频播放总线就绪。")
#             while self.running:
#                 audio_chunk = self.Audio_Chunk_Queue.get()
#                 if audio_chunk is None:
#                     break
                
#                 # 阻塞式播放这一小片数据
#                 # 真实场景为 stream.write(audio_chunk)
#                 time.sleep(0.05) # 模拟播放消耗的时间
#                 # sys.stdout.write("♪")
#                 # sys.stdout.flush()
                
#         except Exception as e:
#             print(f"\n[Thread_Player] 发生异常: {e}")
#             traceback.print_exc()

#     # =========================================================================
#     # Mock 函数：模拟各种流式接口的真实时序行为 (Yield 返回)
#     # =========================================================================
#     def _mock_asr_stream(self):
#         """模拟流式 ASR 获取。一开始是不完整的，后面给出 is_final=True"""
#         simulated_results = [
#             {"text": "给", "is_final": False},
#             {"text": "给我讲", "is_final": False},
#             {"text": "给我讲一个童话", "is_final": False},
#             {"text": "给我讲一个童话故事吧", "is_final": True},
#         ]
#         # 极快的返回节奏模拟
#         for res in simulated_results:
#             time.sleep(0.3)
#             yield res
            
#     def _mock_llm_stream(self, prompt):
#         """模拟流式大模型生成打字机效果。"""
#         simulated_reply = "好的！从前，在一个大森林里，有一只聪明的小狐狸。它最喜欢在春天采蘑菇了！希望你喜欢这个小故事。"
#         # 模拟生成延迟与 Token 逐个吐出的节奏
#         time.sleep(0.5) # Time to First Token (TTFT)
#         for char in simulated_reply:
#             time.sleep(0.05)
#             yield char

#     def _mock_tts_stream(self, sentence):
#         """模拟流式语音合成，一段文本生成多个 Audio Chunks。"""
#         # Time to First Chunk 也是延迟关键
#         time.sleep(0.2) 
#         # 本应返回真实 bytes，这里模拟产生 5 个流式音频片
#         for i in range(5):
#             time.sleep(0.02)
#             yield b"FAKE_AUDIO_CHUNK_BYTES"

# if __name__ == "__main__":
#     system = VoiceInteractionSystem()
#     try:
#         system.start()
#         # 主线程持续休眠，保活子线程
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("\n\n收到退出信号，终止程序执行。")
#         system.stop()
#         sys.exit(0)
