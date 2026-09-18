# -*- coding: utf-8 -*-
"""Unit tests verifying GENERATION_BACKEND_TIMEOUT_SECONDS is propagated to LiteLLM."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.agent.llm_adapter import LLMToolAdapter
from src.analyzer import GeminiAnalyzer
from src.config import Config


def test_analyzer_litellm_timeout_propagation(monkeypatch):
    """Verify GeminiAnalyzer propagates GENERATION_BACKEND_TIMEOUT_SECONDS to LiteLLM and Router."""
    monkeypatch.setenv("GENERATION_BACKEND_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("LITELLM_MODEL", "gemini/gemini-3.7-flash")
    monkeypatch.setenv("GEMINI_API_KEY", "dummy-key-1,dummy-key-2")

    config = Config._load_from_env()
    assert config.generation_backend_timeout_seconds == 45

    with patch("src.analyzer.Router") as mock_router:
        mock_router_instance = MagicMock()
        mock_router.return_value = mock_router_instance

        analyzer = GeminiAnalyzer(config=config)

        import litellm
        assert getattr(litellm, "request_timeout", None) == 45.0
        assert mock_router.call_args is not None
        assert mock_router.call_args.kwargs.get("timeout") == 45.0


def test_analyzer_call_litellm_includes_timeout(monkeypatch):
    """Verify GeminiAnalyzer._call_litellm_impl passes timeout into call_kwargs."""
    monkeypatch.setenv("GENERATION_BACKEND_TIMEOUT_SECONDS", "75")
    monkeypatch.setenv("LITELLM_MODEL", "gemini/gemini-3.7-flash")
    monkeypatch.setenv("GEMINI_API_KEY", "dummy-key")

    config = Config._load_from_env()

    with patch("litellm.completion") as mock_completion:
        mock_response = MagicMock()
        mock_response.choices = [SimpleNamespace(message=SimpleNamespace(content="test response"))]
        mock_response.usage = SimpleNamespace(prompt_tokens=10, completion_tokens=20, total_tokens=30)
        mock_completion.return_value = mock_response

        analyzer = GeminiAnalyzer(config=config)

        res = analyzer.generate_text("test prompt")
        assert res == "test response"

        assert mock_completion.call_args is not None
        call_kwargs = mock_completion.call_args.kwargs
        assert call_kwargs.get("timeout") == 75.0


def test_llm_adapter_timeout_propagation(monkeypatch):
    """Verify LLMToolAdapter passes default timeout into call_kwargs."""
    monkeypatch.setenv("GENERATION_BACKEND_TIMEOUT_SECONDS", "50")
    monkeypatch.setenv("AGENT_LITELLM_MODEL", "gemini/gemini-3.7-flash")
    monkeypatch.setenv("GEMINI_API_KEY", "dummy-key")

    config = Config._load_from_env()

    with patch("litellm.completion") as mock_completion:
        mock_response = MagicMock()
        mock_response.choices = [SimpleNamespace(message=SimpleNamespace(content="agent response", tool_calls=None))]
        mock_response.usage = SimpleNamespace(prompt_tokens=10, completion_tokens=20, total_tokens=30)
        mock_completion.return_value = mock_response

        adapter = LLMToolAdapter(config=config)

        resp = adapter.call_text([{"role": "user", "content": "hello"}])
        assert resp.content == "agent response"

        assert mock_completion.call_args is not None
        call_kwargs = mock_completion.call_args.kwargs
        assert call_kwargs.get("timeout") == 50.0
