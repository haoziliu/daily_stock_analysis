# -*- coding: utf-8 -*-
"""Unit tests for VIX market sentiment integration and US market review prompt."""

import pytest
from src.core.market_profile import US_PROFILE
from src.market_analyzer import (
    MarketAnalyzer,
    MarketIndex,
    MarketOverview,
    format_vix_sentiment,
)
from src.analyzer import GeminiAnalyzer


def test_format_vix_sentiment_tiers():
    """Test all 4 volatility tiers of VIX sentiment in zh and en."""
    # Tier 1: Complacency (< 15)
    vix_low = MarketIndex(code="VIX", name="波动率指数", current=13.5, change_pct=-2.5)
    zh_low = format_vix_sentiment(vix_low, "zh")
    assert "低波动" in zh_low
    assert "13.50" in zh_low
    assert "↓2.50%" in zh_low

    en_low = format_vix_sentiment(vix_low, "en")
    assert "Low Volatility / Complacency" in en_low
    assert "13.50" in en_low

    # Tier 2: Normal (15 - 20)
    vix_norm = MarketIndex(code="VIX", name="波动率指数", current=17.8, change_pct=1.2)
    zh_norm = format_vix_sentiment(vix_norm, "zh")
    assert "常态波动" in zh_norm
    assert "↑1.20%" in zh_norm

    en_norm = format_vix_sentiment(vix_norm, "en")
    assert "Normal / Controlled Risk" in en_norm

    # Tier 3: Elevated / Caution (20 - 30)
    vix_elevated = MarketIndex(code="VIX", name="波动率指数", current=24.1, change_pct=8.5)
    zh_elev = format_vix_sentiment(vix_elevated, "zh")
    assert "避险情绪升温" in zh_elev
    assert "↑8.50%" in zh_elev

    en_elev = format_vix_sentiment(vix_elevated, "en")
    assert "Elevated Volatility / Caution" in en_elev

    # Tier 4: Extreme Fear (>= 30)
    vix_extreme = MarketIndex(code="VIX", name="波动率指数", current=36.0, change_pct=15.0)
    zh_ext = format_vix_sentiment(vix_extreme, "zh")
    assert "极度恐慌" in zh_ext

    en_ext = format_vix_sentiment(vix_extreme, "en")
    assert "Extreme Fear / Panic" in en_ext

    # Invalid / non-positive
    vix_zero = MarketIndex(code="VIX", name="波动率指数", current=0.0)
    assert format_vix_sentiment(vix_zero, "zh") == ""


def test_build_review_prompt_includes_vix_when_present():
    """Test that _build_review_prompt seamlessly embeds VIX sentiment when VIX is in indices."""
    analyzer = MarketAnalyzer(region="us")
    overview = MarketOverview(
        date="2026-09-17",
        indices=[
            MarketIndex(code="SPX", name="标普500", current=5500.0, change_pct=0.5),
            MarketIndex(code="IXIC", name="纳斯达克", current=17500.0, change_pct=-0.2),
            MarketIndex(code="VIX", name="波动率指数", current=18.5, change_pct=3.0),
        ],
    )
    prompt = analyzer._build_review_prompt(overview, news=[])

    assert "## 市场波动率与情绪 (VIX)" in prompt
    assert "VIX 点位：18.50" in prompt
    assert "常态波动/风险可控" in prompt


def test_build_review_prompt_without_vix_graceful():
    """Test that _build_review_prompt works gracefully without VIX in indices."""
    analyzer = MarketAnalyzer(region="cn")
    overview = MarketOverview(
        date="2026-09-17",
        indices=[
            MarketIndex(code="000001", name="上证指数", current=3000.0, change_pct=0.8),
        ],
    )
    prompt = analyzer._build_review_prompt(overview, news=[])

    assert "## 市场波动率与情绪 (VIX)" not in prompt
    assert "上证指数: 3000.00" in prompt


def test_us_profile_enriched_news_queries():
    """Test that US_PROFILE news queries include FOMC / macro catalysts."""
    queries = " ".join(US_PROFILE.news_queries)
    assert "美联储" in queries
    assert "FOMC" in queries
    assert "earnings" in queries
    assert "VIX" in US_PROFILE.prompt_index_hint


def test_analyzer_prompt_contains_styles_and_decoupling_guidance():
    """Test that Analyzer SYSTEM_PROMPT defines styles and short-vs-mid term decoupling."""
    system_prompt = GeminiAnalyzer.SYSTEM_PROMPT
    assert '"styles": {' in system_prompt
    assert '"aggressive": {' in system_prompt
    assert '"balanced": {' in system_prompt
    assert '"conservative": {' in system_prompt
    assert "周期与风格解耦指引" in system_prompt
    assert "双周期视角" in system_prompt
    assert "大盘偏弱时的风格分流与个股独立表达" in system_prompt
