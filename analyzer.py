import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

class DeepSeekAnalyzer:
    def __init__(self, api_key: str, config: dict):
        self.client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1", timeout=30)
        self.model = config.get('model', 'deepseek-chat')
        self.temperature = config.get('temperature', 0.3)

    def analyze_news(self, news: dict) -> dict:
        prompt = f"""分析新闻：{news['title']}。输出JSON：{{"sentiment":"利好/利空/中性","affected_industries":[],"beneficial_sectors":[],"related_funds_stocks":[],"score":1-10,"confidence":0-1,"expectation":"超预期/部分预期/已充分预期"}}"""
        for attempt in range(3):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=self.temperature,
                    response_format={"type": "json_object"}
                )
                result = json.loads(response.choices[0].message.content)
                # 默认值填充
                default = {"sentiment":"中性","affected_industries":[],"beneficial_sectors":[],"related_funds_stocks":[],"score":5,"confidence":0.5,"expectation":"部分预期"}
                default.update(result)
                return default
            except Exception as e:
                logger.warning(f"AI分析重试 {attempt+1}/3: {e}")
                if attempt == 2:
                    return {"sentiment":"中性","affected_industries":[],"beneficial_sectors":[],"related_funds_stocks":[],"score":5,"confidence":0.5,"expectation":"部分预期"}
        return {}
