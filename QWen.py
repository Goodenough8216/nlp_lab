from http import HTTPStatus
import dashscope
import os

# 推荐做法：从环境变量读取 API Key（防止代码泄露）
# 如果你不想配置环境变量，可以先直接写死，例如：
dashscope.api_key = 'sk-190abd83d41b428b845842131baa952b'
# dashscope.api_key = os.getenv("DASHSCOPE_API_KEY") 

def call_qwen():
    messages = [
        {'role': 'system', 'content': 'You are a helpful assistant.'},
        {'role': 'user', 'content': '如何做西红柿鸡蛋？'}
    ]

    response = dashscope.Generation.call(
        model='qwen-turbo',
        messages=messages,
        result_format='message'  # 设置返回格式为 "message"
    )

    if response.status_code == HTTPStatus.OK:
        # 直接提取核心回复内容
        print(response.output.choices[0].message.content)
    else:
        print('Request id: %s, Status code: %s, error code: %s, error message: %s' % (
            response.request_id, 
            response.status_code,
            response.code, 
            response.message
        ))

if __name__ == '__main__':
    call_qwen()