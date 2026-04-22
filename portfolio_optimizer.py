import numpy as np

class PortfolioOptimizer:
    """基于风险平价或等权重的组合优化"""
    
    @staticmethod
    def risk_parity(risks):
        """风险平价：根据风险倒数为权重"""
        inv_risk = 1 / np.array(risks)
        weights = inv_risk / inv_risk.sum()
        return weights
    
    @staticmethod
    def equal_weight(n):
        return np.ones(n) / n
    
    @staticmethod
    def black_litterman(prior_weights, views, view_confidences):
        """简化的Black-Litterman（需协方差矩阵，这里略）"""
        # 示例：直接用观点调整
        adjusted = prior_weights.copy()
        for i, view in enumerate(views):
            adjusted[view['asset_index']] += view['delta'] * view_confidences[i]
        adjusted = np.maximum(adjusted, 0)
        adjusted /= adjusted.sum()
        return adjusted
    
    @staticmethod
    def allocate_cash(industries_scores, total_cash, market_risk_level):
        """根据行业得分和风险偏好分配现金"""
        # 得分归一化
        scores = [score for _, score in industries_scores]
        total = sum(scores)
        if total == 0:
            return []
        raw_weights = [s / total for s in scores]
        # 根据市场风险调整：高风险降低权益仓位，增加债券/现金
        risk_multiplier = {"high": 0.5, "medium": 0.8, "low": 1.0}.get(market_risk_level, 0.8)
        equity_ratio = risk_multiplier
        bond_ratio = 1 - equity_ratio
        
        allocations = []
        for (ind, score), weight in zip(industries_scores, raw_weights):
            amount = total_cash * equity_ratio * weight
            # 映射行业到具体基金（示例映射）
            fund = PortfolioOptimizer.map_industry_to_fund(ind)
            if fund:
                allocations.append({
                    "code": fund["code"],
                    "name": fund["name"],
                    "amount": int(amount),
                    "reason": f"行业{ind}综合得分{score:.2f}"
                })
        # 如果债券比例>0，添加债券基金
        if bond_ratio > 0.05:
            bond_fund = {"code": "040040", "name": "华安纯债债券A"}
            bond_amount = int(total_cash * bond_ratio)
            allocations.append({
                "code": bond_fund["code"],
                "name": bond_fund["name"],
                "amount": bond_amount,
                "reason": f"市场风险{market_risk_level}，配置债券防御"
            })
        return allocations
    
    @staticmethod
    def map_industry_to_fund(industry):
        """行业到基金的映射（可维护）"""
        mapping = {
            "新能源": {"code": "516160", "name": "南方新能源ETF"},
            "半导体": {"code": "512480", "name": "国联安半导体ETF"},
            "消费": {"code": "110022", "name": "易方达消费行业"},
            "医药": {"code": "512010", "name": "易方达沪深300医药ETF"},
            "金融": {"code": "512880", "name": "国泰中证全指证券公司ETF"},
            "周期": {"code": "160416", "name": "华安标普全球石油"},
            "科技": {"code": "515050", "name": "华夏中证5G通信ETF"},
            "军工": {"code": "512660", "name": "国泰中证军工ETF"},
            "地产": {"code": "512200", "name": "南方中证房地产ETF"},
            "农业": {"code": "159825", "name": "富国中证农业ETF"}
        }
        return mapping.get(industry)
