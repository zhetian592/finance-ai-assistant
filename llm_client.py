import time
import logging
import os
from openai import OpenAI

logger = logging.getLogger(__name__)

# 使用 GitHub Models，免费稳定
PRIMARY_MODEL = "gpt-4o-mini"           # 或 "meta-llama-3.1-8b-instruct"
SECONDARY_MODEL = "meta-llama-3.1-8b-instruct"

_clients = {}

def _get_clients():
    if not _clients:
        token = os.getenv("GH_MODELS_TOKEN")
        if not token:
            logger.warning("GH_MODELS_TOKEN 未设置")
            return None

        _clients["primary"] = OpenAI(
            api_key=token,
            base_url="https://models.inference.ai.azure.com"
        )
        _clients["secondary"] = _clients["primary"]  # 备用也用同一个，避免无密钥

    return _clients

def llm_chat(messages, temperature=0.3, max_tokens=2048, prefer_primary=True):
    clients = _get_clients()
    if clients is None:
        return None, None

    candidates = [
        ("primary", PRIMARY_MODEL, clients["primary"]),
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
