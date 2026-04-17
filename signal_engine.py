import logging

logger = logging.getLogger(__name__)

class SignalEngine:
    def __init__(self, db, config):
        self.db = db
        self.min_score = config.get('min_score', 8)
        self.min_confidence = config.get('min_confidence', 0.7)

    def generate_signals(self):
        news_list = self.db.get_latest_news(limit=30)
        signals = []
        for row in news_list:
            if row['score'] is None or row['confidence'] is None:
                continue
            if row['score'] >= self.min_score and row['confidence'] >= self.min_confidence:
                position = round((row['score']/10) * row['confidence'] * 0.05 * 100, 1)
                signals.append({
                    'title': row['title'][:60],
                    'score': row['score'],
                    'confidence': row['confidence'],
                    'expectation': row['expectation'] or '部分预期',
                    'position': position
                })
        return signals
