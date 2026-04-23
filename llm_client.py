# llm_client.py
import time
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

# API配置（从环境变量读取更安全）
ZHIPU_API_KEY = "your_zhipu_key"
DEEPSEEK_API_KEY = "your_deepseek_key"

zhipu_client = OpenAI(
    api_key=ZHIPU_API_KEY,
    base_url="https://open.bigmodel.cn/api/paas/v4/"
)

deepseek_client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com/v1"
)

# 模型名称
PRIMARY_MODEL = "glm-4-flash"        # 智谱Flash
SECONDARY_MODEL = "deepseek-chat"    # DeepSeek V3.2

def call_llm(messages, max_retries=3, primary_first=True):
    """
    统一调用入口，自动主备切换。
    返回 (response_text, model_used)
    """
    # 定义尝试序列
    candidates = [
        (PRIMARY_MODEL, zhipu_client),
        (SECONDARY_MODEL, deepseek_client)
    ] if primary_first else [
        (SECONDARY_MODEL, deepseek_client),
        (PRIMARY_MODEL, zhipu_client)
    ]

    last_exception = None
    for model_name, client in candidates:
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"尝试调用 {model_name}，第 {attempt}/{max_retries} 次")
                response = client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=0.3,  # 分析类任务保持理性
                    max_tokens=2048
                )
                content = response.choices[0].message.content
                logger.info(f"{model_name} 调用成功")
                return content, model_name
            except Exception as e:
                logger.warning(f"{model_name} 失败: {e}")
                last_exception = e
                if "rate limit" in str(e).lower():
                    time.sleep(1 + attempt)  # 限流时指数退避
                else:
                    break  # 非限流错误，直接切换备用
        logger.error(f"{model_name} 彻底不可用，切换到下一个模型")
    
    raise RuntimeError(f"所有模型调用失败，最后错误: {last_exception}")
