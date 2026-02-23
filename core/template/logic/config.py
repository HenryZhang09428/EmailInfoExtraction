"""
保险业务配置 (Insurance Business Configuration)
================================================

集中管理所有社保增减员业务的中文关键词、同义词映射和可调阈值。
本模块是整个领域层的"字典"——修改/扩展同义词只需改这一个文件。

依赖: 无内部依赖，仅使用标准库。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple


@dataclass(frozen=True)
class InsuranceConfig:
    """
    集中管理所有社保增减员业务的中文关键词、同义词和可调阈值。

    设计原则:
      - 所有业务逻辑中的"魔法字符串"都集中在此处
      - frozen=True 保证运行时不可变
      - 新增/修改同义词只需改这一个地方
    """
    # -- 意图关键词 --
    add_keywords: Tuple[str, ...] = ("增员", "入职", "新增", "新入职", "新增人员", "加员", "扩员")
    remove_keywords: Tuple[str, ...] = ("减员", "离职", "退场", "辞退", "解除", "退工", "停保", "退保")
    in_service_keywords: Tuple[str, ...] = ("在职", "在岗", "在册")

    # -- 申报类型表头同义词 --
    declare_type_header_synonyms: Tuple[str, ...] = (
        "社保/申报类型", "申报类型", "社保类型", "变动类型", "办理类型", "业务类型", "操作类型",
    )

    # -- 费用年月 --
    fee_month_direct_keys: Tuple[str, ...] = (
        "费用年月", "缴费年月", "参保月份", "申报月份", "社保月份", "所属期", "费用月份", "缴费月份", "参保年月", "申报年月",
    )
    fee_month_header_anchors: Tuple[str, ...] = ("费用", "缴费", "参保", "申报", "社保", "所属")
    fee_month_header_exact: Tuple[str, ...] = (
        "费用年月", "缴费年月", "参保月份", "申报月份", "社保月份", "所属期", "费用月份", "缴费月份",
    )

    # -- 日期字段候选 --
    add_date_keys: Tuple[str, ...] = ("入职日期", "签订日期", "生效日期", "变动日期", "办理日期")
    remove_date_keys: Tuple[str, ...] = ("离职日期", "退保日期", "退场日期", "退工日期", "变动日期", "办理日期")
    add_date_semantic_keys: Tuple[str, ...] = ("start_date", "入职日期", "到岗日期")
    remove_date_semantic_keys: Tuple[str, ...] = ("leave_date", "end_date", "离职日期", "终止日期")

    # -- 姓名字段 --
    name_key_exact: Tuple[str, ...] = ("name",)
    name_key_chinese: Tuple[str, ...] = ("姓名",)

    # -- 模板检测必需表头后缀 --
    required_header_suffixes: Tuple[str, ...] = ("姓名", "申报类型", "费用年月")

    # -- 同义词映射 (fallback 用) --
    synonym_map: Dict[str, Tuple[str, ...]] = field(default_factory=lambda: {
        "身份证号码": ("证件号码", "身份证号", "身份证", "证件号"),
        "证件号码": ("身份证号码", "身份证号", "身份证", "证件号"),
        "姓名": ("名字", "名称"), "名字": ("姓名", "名称"),
        "联系方式": ("电话", "手机", "联系电话", "手机号"),
        "电话": ("联系方式", "手机", "联系电话", "手机号"),
        "入职日期": ("入职时间", "入职"),
        "离职退场时间": ("离职时间", "退场时间", "离职日期", "退场日期"),
    })

    # -- 英文归一化键名 --
    remove_semantic_keys: Tuple[str, ...] = ("terminationdate", "terminationreason")
    add_semantic_keys: Tuple[str, ...] = ("startdate", "employeestatus", "employmentstatus", "monthlycontribution", "contribution", "feemonth")

    # -- 阈值 --
    fuzzy_match_threshold: int = 80
    record_gating_min_ratio: float = 0.4
    record_gating_fallback_top_n: int = 5
    record_trim_limit: int = 30
    max_source_keys_in_profile: int = 60
    sample_records_count: int = 2


# 全局单例
CONFIG = InsuranceConfig()
