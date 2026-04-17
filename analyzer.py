import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

class DeepSeekAnalyzer:
    def __init__(self, api_key: str, config: dict):
        if not api_key or len(api_key) < 20:
            logger.error("API Key 无效，请检查 GitHub Secrets 中的 DEEPSEEK_API_KEY")
        self.client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1")
        self.model = config.get('model', 'deepseek-chat')
        self.temperature = config.get('temperature', 0.3)

    def analyze_news(self, news: dict) -> dict:
        prompt = f"""分析新闻：{news['title']}。输出JSON：{{"sentiment":"利好/利空/中性","affected_industries":[],"beneficial_sectors":[],"related_funds_stocks":[],"score":1-10,"confidence":0-1,"expectation":"超预期/部分预期/已充分预期"}}"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            # 确保字段存在
            for k in ['sentiment', 'affected_industries', 'beneficial_sectors', 'related_funds_stocks', 'score', 'confidence', 'expectation']:
                if k not in result:
                    result[k] = [] if k in ['affected_industries','beneficial_sectors','related_funds_stocks'] else (0.5 if k=='confidence' else (5 if k=='score' else '中性'))
            return result
        except Exception as e:
            logger.error(f"AI分析失败: {e}")
            return {"sentiment":"中性","affected_industries":[],"beneficial_sectors":[],"related_funds_stocks":[],"score":5,"confidence":0.5,"expectation":"部分预期"}
