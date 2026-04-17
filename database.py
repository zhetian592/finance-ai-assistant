# -*- coding: utf-8 -*-
"""
数据库模块：SQLite 操作
"""

import sqlite3
import hashlib
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='data/finance.db'):
        self.db_path = db_path
        self.conn = None
    
    def get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def initialize(self):
        """创建表结构"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 新闻原始表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                time TEXT,
                source TEXT,
                summary TEXT,
                url TEXT,
                fingerprint TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # AI分析结果表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                news_id INTEGER NOT NULL,
                sentiment TEXT,
                affected_industries TEXT,
                beneficial_sectors TEXT,
                related_funds_stocks TEXT,
                score INTEGER,
                confidence REAL,
                expectation TEXT,
                raw_response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (news_id) REFERENCES news_raw(id)
            )
        ''')
        
        # 资金流数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fund_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                type TEXT,
                value REAL,
                sector TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 大盘数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
        ''')
        
        conn.commit()
        logger.info("数据库初始化完成")
    
    def insert_news(self, news: dict) -> int:
        """插入新闻，返回ID；若已存在返回0"""
        fingerprint = self._calc_fingerprint(news)
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO news_raw (title, time, source, summary, url, fingerprint)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (news['title'], news['time'], news['source'], news['summary'], news['url'], fingerprint))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            logger.debug(f"新闻已存在: {news['title'][:30]}")
            return 0
    
    def _calc_fingerprint(self, news: dict) -> str:
        """MD5指纹：标题+来源+发布时间前10分钟"""
        base = f"{news['title']}|{news['source']}|{news['time'][:16] if news['time'] else ''}"
        return hashlib.md5(base.encode()).hexdigest()
    
    def insert_analysis(self, news_id: int, analysis: dict):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO news_analysis 
            (news_id, sentiment, affected_industries, beneficial_sectors, related_funds_stocks, score, confidence, expectation, raw_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            news_id,
            analysis.get('sentiment'),
            ','.join(analysis.get('affected_industries', [])),
            ','.join(analysis.get('beneficial_sectors', [])),
            ','.join(analysis.get('related_funds_stocks', [])),
            analysis.get('score'),
            analysis.get('confidence'),
            analysis.get('expectation'),
            ''
        ))
        conn.commit()
    
    def insert_fund_flow(self, fund_data: dict):
        conn = self.get_connection()
        cursor = conn.cursor()
        # 北向资金
        north = fund_data.get('north_flow', {})
        if north:
            cursor.execute('''
                INSERT INTO fund_flow (date, type, value) VALUES (?, ?, ?)
            ''', (north.get('date'), 'north', north.get('net_inflow', 0)))
        # 板块资金
        for sector_flow in fund_data.get('sector_flows', []):
            cursor.execute('''
                INSERT INTO fund_flow (date, type, value, sector) VALUES (?, ?, ?, ?)
            ''', (sector_flow.get('date'), 'sector', sector_flow.get('net_inflow', 0), sector_flow.get('sector')))
        conn.commit()
    
    def insert_market_data(self, market: dict):
        if not market:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO market_data (date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (market.get('date'), market.get('open'), market.get('high'), market.get('low'), market.get('close'), market.get('volume')))
        conn.commit()
    
    def get_latest_news(self, limit=50):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT n.*, a.sentiment, a.score, a.confidence, a.expectation, a.affected_industries, a.beneficial_sectors
            FROM news_raw n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            ORDER BY n.created_at DESC
            LIMIT ?
        ''', (limit,))
        return cursor.fetchall()
    
    def get_latest_market(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM market_data ORDER BY date DESC LIMIT 5')
        return cursor.fetchall()
    
    def get_recent_fund_flow(self, days=5):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT date, type, value, sector FROM fund_flow
            WHERE date >= date('now', ?)
            ORDER BY date DESC
        ''', (f'-{days} days',))
        return cursor.fetchall()
