import akshare as ak
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def update_fund_nav(holdings_list: List[Dict]) -> List[Dict]:
    """
    更新每只基金的最新单位净值
    使用 akshare 的 fund_individual_basic_info_em 接口
    """
    if not holdings_list:
        return holdings_list

    updated = []
    for fund in holdings_list:
        code = fund.get("code")
        if not code:
            logger.warning("基金缺少 code 字段，跳过")
            updated.append(fund)
            continue

        try:
            # 获取基金基本信息
            df = ak.fund_individual_basic_info_em(fund=code)
            if df.empty:
                logger.warning(f"基金 {code} 未获取到数据")
                updated.append(fund)
                continue

            # 查找净值列
            nav_col = None
            for col in df.columns:
                if '单位净值' in col or '最新净值' in col:
                    nav_col = col
                    break
            if nav_col is None:
                logger.warning(f"基金 {code} 未找到净值列，可用列: {df.columns.tolist()}")
                updated.append(fund)
                continue

            latest_nav = float(df.iloc[0][nav_col])
            fund['current'] = latest_nav
            logger.info(f"基金 {code} 最新净值: {latest_nav}")

        except Exception as e:
            logger.warning(f"更新基金 {code} 净值失败: {e}")

        updated.append(fund)

    return updated
