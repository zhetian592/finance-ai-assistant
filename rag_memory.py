import json
import os
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss

MEMORY_FILE = "backtest/events_memory.json"
INDEX_FILE = "backtest/events_index.faiss"

# 加载嵌入模型（约400MB，首次下载稍慢）
try:
    embedder = SentenceTransformer('all-MiniLM-L6-v2')
    EMBED_AVAILABLE = True
except:
    EMBED_AVAILABLE = False

def store_events(events):
    """存储事件到 JSON 并建立向量索引（用于相似事件检索）"""
    if not EMBED_AVAILABLE:
        return
    # 读取已有事件
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, 'r') as f:
            old_events = json.load(f)
    else:
        old_events = []
    # 去重（按title）
    new_titles = {e["title"] for e in events}
    filtered = [e for e in old_events if e["title"] not in new_titles]
    all_events = filtered + events
    with open(MEMORY_FILE, 'w') as f:
        json.dump(all_events, f, indent=2)
    
    # 重建索引
    texts = [e["title"] + " " + e["summary"] for e in all_events]
    embeddings = embedder.encode(texts)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings.astype('float32'))
    faiss.write_index(index, INDEX_FILE)

def retrieve_similar_events(query_text, top_k=5):
    """根据查询文本检索最相似的历史事件"""
    if not EMBED_AVAILABLE or not os.path.exists(INDEX_FILE):
        return []
    index = faiss.read_index(INDEX_FILE)
    with open(MEMORY_FILE, 'r') as f:
        events = json.load(f)
    query_vec = embedder.encode([query_text])
    distances, indices = index.search(query_vec.astype('float32'), top_k)
    return [events[i] for i in indices[0] if i < len(events)]
