# -*- coding: utf-8 -*-
"""Shared risk override planning for the multi-agent pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from src.agent.protocols import AgentContext, normalize_decision_signal


_DOWNGRADE_STEPS = {
    "downgrade_one": 1,
    "downgrade_two": 2,
}


class DashboardDecisionSignal(str, Enum):
    """Canonical signals used while applying Agent risk controls."""

    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"


class RiskTrigger(str, Enum):
    """Normalized trigger selected for one risk-control evaluation."""

    NONE = "none"
    RISK_VETO = "risk_veto"
    RISK_DOWNGRADE = "risk_downgrade"


class RiskApplicationReason(str, Enum):
    """Exhaustive internal outcomes of evaluating a risk override."""

    NO_RISK_EVIDENCE = "no_risk_evidence"
    NO_OVERRIDE_TRIGGER = "no_override_trigger"
    OVERRIDE_DISABLED = "override_disabled"
    POST_RISK_SIGNAL_ALREADY_WITHIN_RISK_LIMIT = (
        "post_risk_signal_already_within_risk_limit"
    )
    RISK_VETO_APPLIED = "risk_veto_applied"
    RISK_DOWNGRADE_APPLIED = "risk_downgrade_applied"


_APPLIED_REASONS = frozenset({
    RiskApplicationReason.RISK_VETO_APPLIED,
    RiskApplicationReason.RISK_DOWNGRADE_APPLIED,
})
_VALID_DOWNGRADE_TRANSITIONS = frozenset({
    (DashboardDecisionSignal.BUY, DashboardDecisionSignal.HOLD),
    (DashboardDecisionSignal.BUY, DashboardDecisionSignal.SELL),
    (DashboardDecisionSignal.HOLD, DashboardDecisionSignal.SELL),
})


def classify_risk_application_reason(
    *,
    evidence_present: bool,
    trigger: RiskTrigger,
    override_enabled: bool,
    applied: bool,
) -> RiskApplicationReason:
    """Classify one application from normalized runtime facts."""
    trigger = RiskTrigger(trigger)
    if not evidence_present:
        return RiskApplicationReason.NO_RISK_EVIDENCE
    if trigger == RiskTrigger.NONE:
        return RiskApplicationReason.NO_OVERRIDE_TRIGGER
    if not override_enabled:
        return RiskApplicationReason.OVERRIDE_DISABLED
    if not applied:
        return RiskApplicationReason.POST_RISK_SIGNAL_ALREADY_WITHIN_RISK_LIMIT
    if trigger == RiskTrigger.RISK_VETO:
        return RiskApplicationReason.RISK_VETO_APPLIED
    return RiskApplicationReason.RISK_DOWNGRADE_APPLIED


def validate_risk_application_transition(
    *,
    applied: bool,
    reason: RiskApplicationReason,
    post_risk_signal: DashboardDecisionSignal,
    from_signal: Optional[DashboardDecisionSignal],
    to_signal: Optional[DashboardDecisionSignal],
) -> None:
    """Reject internally contradictory application records."""
    reason = RiskApplicationReason(reason)
    post_risk_signal = DashboardDecisionSignal(post_risk_signal)
    from_signal = DashboardDecisionSignal(from_signal) if from_signal is not None else None
    to_signal = DashboardDecisionSignal(to_signal) if to_signal is not None else None

    if not applied:
        if from_signal is not None or to_signal is not None:
            raise ValueError("non-applied risk override cannot carry a signal transition")
        if reason in _APPLIED_REASONS:
            raise ValueError("applied reason requires applied=True")
        return

    if from_signal is None or to_signal is None:
        raise ValueError("applied risk override requires from_signal and to_signal")
    if from_signal == to_signal:
        raise ValueError("applied risk override must change the signal")
    if to_signal != post_risk_signal:
        raise ValueError("to_signal must match post_risk_signal")
    if reason == RiskApplicationReason.RISK_VETO_APPLIED:
        if (from_signal, to_signal) != (
            DashboardDecisionSignal.BUY,
            DashboardDecisionSignal.HOLD,
        ):
            raise ValueError("risk veto application must change buy to hold")
    elif reason == RiskApplicationReason.RISK_DOWNGRADE_APPLIED:
        if (from_signal, to_signal) not in _VALID_DOWNGRADE_TRANSITIONS:
            raise ValueError("risk downgrade must move to a more conservative signal")
    else:
        raise ValueError("applied risk override requires an applied reason")


@dataclass(frozen=True)
class RiskOverridePlan:
    """Configuration-aware risk override decision shared by summary and executor."""

    evidence_present: bool
    override_enabled: bool
    override_trigger_present: bool
    veto_buy: bool
    adjustment: str
    has_high_flag: bool
    risk_level_high: bool
    current_signal: Optional[str]
    target_signal: Optional[str]
    will_apply: Optional[bool]
    reason: str

    @property
    def trigger(self) -> RiskTrigger:
        """Return the effective trigger using execution precedence."""
        if self.veto_buy and self.current_signal == DashboardDecisionSignal.BUY:
            return RiskTrigger.RISK_VETO
        if self.adjustment in _DOWNGRADE_STEPS:
            return RiskTrigger.RISK_DOWNGRADE
        if self.veto_buy:
            return RiskTrigger.RISK_VETO
        return RiskTrigger.NONE

    def to_low_sensitivity_dict(self) -> Dict[str, Any]:
        """Return a prompt-safe view that does not expose raw risk payloads."""
        return {
            "evidence_present": self.evidence_present,
            "override_enabled": self.override_enabled,
            "override_trigger_present": self.override_trigger_present,
            "veto_buy": self.veto_buy,
            "will_apply": self.will_apply,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class RiskOverrideApplication:
    """Validated low-sensitivity result of a risk-control evaluation."""

    evidence_present: bool
    override_enabled: bool
    trigger: RiskTrigger
    applied: bool
    reason: RiskApplicationReason
    post_risk_signal: DashboardDecisionSignal
    from_signal: Optional[DashboardDecisionSignal] = None
    to_signal: Optional[DashboardDecisionSignal] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "trigger", RiskTrigger(self.trigger))
        object.__setattr__(self, "reason", RiskApplicationReason(self.reason))
        object.__setattr__(
            self,
            "post_risk_signal",
            DashboardDecisionSignal(self.post_risk_signal),
        )
        if self.from_signal is not None:
            object.__setattr__(self, "from_signal", DashboardDecisionSignal(self.from_signal))
        if self.to_signal is not None:
            object.__setattr__(self, "to_signal", DashboardDecisionSignal(self.to_signal))

        if not self.evidence_present and self.trigger != RiskTrigger.NONE:
            raise ValueError("risk trigger requires risk evidence")
        expected_reason = classify_risk_application_reason(
            evidence_present=self.evidence_present,
            trigger=self.trigger,
            override_enabled=self.override_enabled,
            applied=self.applied,
        )
        if self.reason != expected_reason:
            raise ValueError(
                f"risk application reason must be {expected_reason.value} for the supplied facts"
            )
        validate_risk_application_transition(
            applied=self.applied,
            reason=self.reason,
            post_risk_signal=self.post_risk_signal,
            from_signal=self.from_signal,
            to_signal=self.to_signal,
        )


def build_risk_override_application(plan: RiskOverridePlan) -> RiskOverrideApplication:
    """Build the actual outcome for a plan evaluated against a dashboard signal."""
    if plan.current_signal is None or plan.target_signal is None or plan.will_apply is None:
        raise ValueError("risk override application requires an evaluated current signal")

    current_signal = DashboardDecisionSignal(plan.current_signal)
    target_signal = DashboardDecisionSignal(plan.target_signal)
    reason = classify_risk_application_reason(
        evidence_present=plan.evidence_present,
        trigger=plan.trigger,
        override_enabled=plan.override_enabled,
        applied=plan.will_apply,
    )
    if plan.will_apply:
        return RiskOverrideApplication(
            evidence_present=plan.evidence_present,
            override_enabled=plan.override_enabled,
            trigger=plan.trigger,
            applied=True,
            reason=reason,
            post_risk_signal=target_signal,
            from_signal=current_signal,
            to_signal=target_signal,
        )
    return RiskOverrideApplication(
        evidence_present=plan.evidence_present,
        override_enabled=plan.override_enabled,
        trigger=plan.trigger,
        applied=False,
        reason=reason,
        post_risk_signal=current_signal,
    )


def build_risk_override_plan(
    ctx: AgentContext,
    *,
    current_signal: Any = None,
    override_enabled: bool = True,
) -> RiskOverridePlan:
    """Build the single source of truth for risk override decisions.

    ``risk_level=high`` is risk evidence, but it is not by itself an override
    trigger. Actual execution also depends on ``override_enabled`` and on the
    dashboard signal observed before applying the risk rule.
    """
    risk_raw = _latest_risk_raw(ctx)
    adjustment = str(risk_raw.get("signal_adjustment") or "").strip().lower()
    has_high_flag = any(
        str(flag.get("severity", "")).strip().lower() == "high"
        for flag in ctx.risk_flags
        if isinstance(flag, dict)
    )
    risk_level_high = str(risk_raw.get("risk_level") or "").strip().lower() == "high"
    veto_buy = bool(risk_raw.get("veto_buy")) or adjustment == "veto" or has_high_flag
    has_downgrade = adjustment in _DOWNGRADE_STEPS
    override_trigger_present = veto_buy or has_downgrade
    evidence_present = override_trigger_present or risk_level_high

    normalized_current = (
        normalize_decision_signal(current_signal)
        if isinstance(current_signal, str)
        else None
    )
    target_signal = normalized_current
    will_apply: Optional[bool]

    if normalized_current is None:
        will_apply = None
    elif not override_enabled or not override_trigger_present:
        will_apply = False
    else:
        if veto_buy and normalized_current == "buy":
            target_signal = "hold"
        elif has_downgrade:
            target_signal = _downgrade_signal(
                normalized_current,
                steps=_DOWNGRADE_STEPS[adjustment],
            )
        will_apply = target_signal != normalized_current

    return RiskOverridePlan(
        evidence_present=evidence_present,
        override_enabled=bool(override_enabled),
        override_trigger_present=override_trigger_present,
        veto_buy=veto_buy,
        adjustment=adjustment,
        has_high_flag=has_high_flag,
        risk_level_high=risk_level_high,
        current_signal=normalized_current,
        target_signal=target_signal,
        will_apply=will_apply,
        reason=_risk_override_reason(
            veto_buy=veto_buy,
            adjustment=adjustment,
            has_high_flag=has_high_flag,
            risk_level_high=risk_level_high,
        ),
    )


def _latest_risk_raw(ctx: AgentContext) -> Dict[str, Any]:
    risk_opinion = next((op for op in reversed(ctx.opinions) if op.agent_name == "risk"), None)
    if risk_opinion and isinstance(risk_opinion.raw_data, dict):
        return risk_opinion.raw_data
    return {}


def _risk_override_reason(
    *,
    veto_buy: bool,
    adjustment: str,
    has_high_flag: bool,
    risk_level_high: bool,
) -> str:
    if has_high_flag:
        return "high_severity_flag"
    if veto_buy:
        return "risk_veto"
    if adjustment in _DOWNGRADE_STEPS:
        return adjustment
    if risk_level_high:
        return "high_risk_evidence"
    return "none"


def _downgrade_signal(signal: str, steps: int = 1) -> str:
    order = ["buy", "hold", "sell"]
    try:
        index = order.index(signal)
    except ValueError:
        return signal
    return order[min(len(order) - 1, index + max(0, steps))]


def build_risk_style_options(
    dashboard: Dict[str, Any],
    *,
    default_position: str = "",
    stop_loss_ref: Optional[Union[str, float, int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Build 3 risk-style options (balanced, aggressive, conservative) based on baseline analysis.
    """
    if not isinstance(dashboard, dict):
        dashboard = {}
    battle_plan = dashboard.get("battle_plan") or {}
    if not isinstance(battle_plan, dict):
        battle_plan = {}
    pos_strat = battle_plan.get("position_strategy") or {}
    if not isinstance(pos_strat, dict):
        pos_strat = {}
    sniper_points = battle_plan.get("sniper_points") or {}
    if not isinstance(sniper_points, dict):
        sniper_points = {}
    core_conclusion = dashboard.get("core_conclusion") or {}
    if not isinstance(core_conclusion, dict):
        core_conclusion = {}
    pos_advice = core_conclusion.get("position_advice") or {}
    if not isinstance(pos_advice, dict):
        pos_advice = {}

    base_pos = pos_strat.get("suggested_position") or default_position or "建议仓位：2-3成 (25%)"
    base_entry = pos_strat.get("entry_plan") or "支撑位分批吸筹/低吸"
    base_risk = pos_strat.get("risk_control") or "破位止损，防范流动性回撤"
    base_no_pos = pos_advice.get("no_position") or "支撑位分批建仓"

    # 如果 LLM 已经生成了自定义的 styles 且包含完整建议，直接优先复用 LLM 的原生深度分析
    existing_styles = pos_strat.get("styles")
    if isinstance(existing_styles, dict):
        agg_item = existing_styles.get("aggressive")
        cons_item = existing_styles.get("conservative")
        bal_item = existing_styles.get("balanced")
        if isinstance(agg_item, dict) and agg_item.get("position_advice") and isinstance(cons_item, dict) and cons_item.get("position_advice"):
            labels = {"balanced": "⚖️ 稳健型 (默认)", "aggressive": "🚀 激进型", "conservative": "🛡️ 保守型"}
            res = {}
            for key in ["balanced", "aggressive", "conservative"]:
                src = existing_styles.get(key) if isinstance(existing_styles.get(key), dict) else {}
                res[key] = {
                    "label": src.get("label") or labels[key],
                    "suggested_position": src.get("suggested_position") or (pos_strat.get("suggested_position") or default_position or "按需建仓"),
                    "position_advice": src.get("position_advice") or (pos_advice.get("no_position") or "按策略执行"),
                    "stop_loss": src.get("stop_loss") or str(sniper_points.get("stop_loss") or stop_loss_ref or "MA20支撑"),
                    "entry_strategy": src.get("entry_strategy") or (pos_strat.get("entry_plan") or "分批建仓"),
                    "risk_control": src.get("risk_control") or (pos_strat.get("risk_control") or "破位风控"),
                }
            return res

    stop_loss_raw = str(sniper_points.get("stop_loss") or stop_loss_ref or "跌破止损位/MA20")
    target_raw = str(sniper_points.get("take_profit") or "阻力位")
    
    # 提取行情状态 (得分、趋势预测、操作建议)
    overall_score = 50
    try:
        score_val = dashboard.get("overall_score")
        if score_val is not None:
            overall_score = float(score_val)
    except (ValueError, TypeError):
        pass

    action_label = str(dashboard.get("overall_advice") or pos_strat.get("action") or "").lower()
    is_bullish = overall_score >= 60 or "买入" in action_label or "看多" in action_label
    is_bearish = overall_score < 40 or "卖出" in action_label or "避险" in action_label

    import re

    def _clean_price_only(raw_str: str) -> str:
        if not raw_str:
            return ""
        s = str(raw_str).strip()
        for prefix in ["止损位：", "止损价：", "目标位：", "目标价：", "理想买入：", "次优买入：", "止损参考："]:
            s = s.replace(prefix, "")
        s = re.sub(r'[\(（].*?[\)）]', '', s).strip()
        return s or str(raw_str).strip()

    stop_loss_val = _clean_price_only(stop_loss_raw) or "支撑位"
    target_val = _clean_price_only(target_raw) or "阻力位"

    if is_bullish:
        # 多头突破/强研判场景
        agg_advice = f"若放量有效突破阻力位（{target_val}），可重仓5-6成顺势追击主升浪；跌破{stop_loss_val}立即锁定利润"
        cons_advice = f"多头趋势中不盲目追高，等待股价缩量回踩强支撑位（{stop_loss_val}）企稳后以1-2成轻仓参与"
        agg_risk = "关注高位放量滞涨风险，移动止盈保护浮盈"
        cons_risk = "严格分批建仓，严禁在阻力位附近追高"
    elif is_bearish:
        # 弱势空头/防守场景
        agg_advice = f"仅限超跌反弹短线博弈（仓位控制在3-4成），反弹至阻力位（{target_val}）附近必须快进快出"
        cons_advice = f"趋势偏弱，建议持续空仓观望；无有效站稳支撑位（{stop_loss_val}）信号前坚决不建仓"
        agg_risk = "短线快进快出，严禁深套，一旦破位无条件切仓"
        cons_risk = "本金安全第一，严格保持空仓防守纪律"
    else:
        # 震荡盘整/箱体场景
        agg_advice = f"围绕[{stop_loss_val}, {target_val}]箱体波段操作；若向上突破箱体顶端（{target_val}）追加至5成仓位"
        cons_advice = f"仅在股价缩量触及箱体下沿支撑位（{stop_loss_val}）并出现止跌K线时，动用1-2成仓试错"
        agg_risk = "防范箱体假突破洗盘，触及箱体上沿及时分批减仓"
        cons_risk = "控制最大回撤，破位下沿无条件清仓观望"

    return {
        "balanced": {
            "label": "⚖️ 稳健型 (默认)",
            "suggested_position": base_pos,
            "position_advice": base_no_pos,
            "stop_loss": stop_loss_val,
            "entry_strategy": base_entry,
            "risk_control": base_risk,
        },
        "aggressive": {
            "label": "🚀 激进型",
            "suggested_position": "建议仓位：5-6成 (55%)" if not is_bearish else "建议仓位：3-4成 (35%)",
            "position_advice": agg_advice,
            "stop_loss": f"{stop_loss_val} (放宽 1-2% 容忍短线洗盘)" if not is_bearish else f"{stop_loss_val} (严格紧贴支撑线止损)",
            "entry_strategy": "突破建仓 / 动能确认后快速全量入场",
            "risk_control": agg_risk,
        },
        "conservative": {
            "label": "🛡️ 保守型",
            "suggested_position": "建议仓位：1-2成 (15%)" if not is_bearish else "建议仓位：0-1成 (5%)",
            "position_advice": cons_advice,
            "stop_loss": f"{stop_loss_val} (收紧硬止损触发即走)",
            "entry_strategy": "支撑位低吸分批挂单 / 无企稳信号绝不建仓",
            "risk_control": cons_risk,
        },
    }


__all__ = [
    "DashboardDecisionSignal",
    "RiskApplicationReason",
    "RiskOverrideApplication",
    "RiskOverridePlan",
    "RiskTrigger",
    "build_risk_override_application",
    "build_risk_override_plan",
    "build_risk_style_options",
    "classify_risk_application_reason",
    "validate_risk_application_transition",
]
