# quant/market_data.py (替换 get_north_flow 函数)

def get_north_flow(days=5):
    """获取北向资金净流入（自动适配列名）"""
    try:
        df = ak.stock_hsgt_hist_em()
        if df.empty:
            return None
        
        # 打印列名用于调试（可选，可注释）
        # print("北向资金列名:", df.columns.tolist())
        
        # 自动查找包含“净买入”的列（万元或亿元）
        net_col = None
        for col in df.columns:
            if '净买入' in col and '万元' in col:
                net_col = col
                break
        if net_col is None:
            # 尝试其他可能列名
            if '北向资金净买入(万元)' in df.columns:
                net_col = '北向资金净买入(万元)'
            elif '净买入(万元)' in df.columns:
                net_col = '净买入(万元)'
            else:
                # 如果都没有，尝试使用第一个数值列（不推荐，但容错）
                numeric_cols = df.select_dtypes(include='number').columns
                if len(numeric_cols) > 0:
                    net_col = numeric_cols[0]
                    logger.warning(f"未找到净买入列，使用 {net_col} 代替")
                else:
                    raise KeyError("无法识别北向资金净买入列")
        
        # 计算最近N日净流入总额（单位：亿元）
        recent = df.head(days)
        total_net = recent[net_col].sum() / 10000  # 从万元转为亿元
        
        return {
            "total_net_billion": round(total_net, 2),
            "data_date": datetime.now().strftime("%Y-%m-%d")
        }
    except Exception as e:
        logger.warning(f"获取北向资金失败: {e}")
        return None
