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
        zhipu_key = os.getenv("ZHIPU_API_KEY")
        deepseek_key = os.getenv("DEEPSEEK_API_KEY")

        if not zhipu_key and not deepseek_key:
            logger.error("ZHIPU_API_KEY 和 DEEPSEEK_API_KEY 均未设置！情感分析将退回中性值。")
            return None

        if zhipu_key:
            _clients["primary"] = OpenAI(
                api_key=zhipu_key,
                base_url="https://open.bigmodel.cn/api/paas/v4/"
            )
        if deepseek_key:
            _clients["secondary"] = OpenAI(
                api_key=deepseek_key,
                base_url="https://api.deepseek.com/v1"
            )
    return _clients if _clients else None

def llm_chat(messages, temperature=0.3, max_tokens=2048, prefer_primary=True):
    """
    主备自动切换。返回 (content, model_used)。
    如果所有模型都不可用，返回 (None, None)。
    """
    clients = _get_clients()
    if clients is None:
        return None, None

    candidates = [
        ("primary", PRIMARY_MODEL, clients.get("primary")),
        ("secondary", SECONDARY_MODEL, clients.get("secondary"))
    ] if prefer_primary else [
        ("secondary", SECONDARY_MODEL, clients.get("secondary")),
        ("primary", PRIMARY_MODEL, clients.get("primary"))
    ]

    last_err = None
    for role, model_name, client in candidates:
        if client is None:
            continue
        for attempt in range(3):
            try:
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
                logger.warning(f"{model_name} 第{attempt+1}次失败: {e}")
                last_err = e
                if "rate" in str(e).lower():
                    time.sleep(1 + attempt)
                else:
                    break

    logger.error(f"所有模型调用失败，最后错误: {last_err}")
    return None, None
