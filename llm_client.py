import time
import logging
import os
from openai import OpenAI

logger = logging.getLogger(__name__)

PRIMARY_MODEL = "glm-4-flash"
SECONDARY_MODEL = "deepseek-chat"

_clients = {}

def _get_clients():
    if not _clients:
        _clients["primary"] = OpenAI(
            api_key=os.getenv("ZHIPU_API_KEY"),
            base_url="https://open.bigmodel.cn/api/paas/v4/"
        )
        _clients["secondary"] = OpenAI(
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url="https://api.deepseek.com/v1"
        )
    return _clients

def llm_chat(messages, temperature=0.3, max_tokens=2048, prefer_primary=True):
    """
    主备模型自动切换。优先调用 primary (glm-4-flash)，失败则切换 secondary (deepseek-chat)。
    返回 (content: str, model_used: str)
    """
    clients = _get_clients()
    candidates = [
        ("primary", PRIMARY_MODEL, clients["primary"]),
        ("secondary", SECONDARY_MODEL, clients["secondary"])
    ] if prefer_primary else [
        ("secondary", SECONDARY_MODEL, clients["secondary"]),
        ("primary", PRIMARY_MODEL, clients["primary"])
    ]

    last_err = None
    for role, model_name, client in candidates:
        for attempt in range(3):
            try:
                logger.debug(f"尝试调用 {model_name}，第{attempt+1}次")
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                content = resp.choices[0].message.content
                logger.info(f"{model_name} 调用成功")
                return content, model_name
            except Exception as e:
                logger.warning(f"{model_name} 调用失败: {e}")
                last_err = e
                if "rate" in str(e).lower():
                    time.sleep(1 + attempt)
                else:
                    break   # 非限流错误直接切换
    raise RuntimeError(f"所有模型调用失败，最后错误: {last_err}")
