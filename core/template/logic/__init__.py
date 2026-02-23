"""
模板填充逻辑包 (Template Fill Logic Package)
=============================================

分层结构:
  config.py     — InsuranceConfig 业务配置 + CONFIG 单例
  common.py     — 通用核心层 (Layer 1): 不含业务关键词的通用类与工具函数
  insurance.py  — 保险业务域层 (Layer 2): 社保增减员的业务规则与领域模型

依赖方向: common.py ← insurance.py  (单向; common 不引用 insurance)
"""
