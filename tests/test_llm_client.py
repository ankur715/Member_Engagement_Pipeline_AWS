import pytest
from pydantic import BaseModel

from fake_llm import FakeClient, parsed_response, refusal_response, text_response
from pipeline import config
from pipeline.ai import llm


class Answer(BaseModel):
    value: str


@pytest.fixture
def use_fake(monkeypatch):
    """Turn the LLM on with a fake client; returns a function to set the responder."""
    def install(provider, responder):
        monkeypatch.setattr(config, "LLM_PROVIDER", provider)
        fake = FakeClient(responder)
        monkeypatch.setattr(llm, "client", lambda: fake)
        return fake
    return install


def test_disabled_by_default():
    assert not llm.enabled()
    with pytest.raises(llm.LLMUnavailable):
        llm.parse("s", "u", Answer)


def test_parse_on_claude_api_sets_model_effort_and_fallbacks(use_fake):
    fake = use_fake("anthropic", lambda kind, kw: parsed_response(Answer(value="ok")))
    usage = llm.Usage()
    result = llm.parse("system prompt", "question", Answer, usage=usage)
    assert result.parsed == Answer(value="ok") and not result.refused
    kind, kw = fake.messages.calls[0]
    assert kind == "parse" and kw["model"] == "claude-opus-5-5"
    assert kw["output_format"] is Answer and kw["output_config"] == {"effort": "low"}
    assert kw["fallbacks"] == "default" and kw["betas"] == ["server-side-fallback-2026-07-01"]
    assert (usage.requests, usage.input_tokens, usage.output_tokens) == (1, 100, 20)


def test_bedrock_uses_prefixed_model_and_no_fallback_param(use_fake):
    fake = use_fake("bedrock", lambda kind, kw: parsed_response(Answer(value="ok")))
    llm.parse("s", "u", Answer)
    _, kw = fake.messages.calls[0]
    assert kw["model"] == "anthropic.claude-opus-5-5"
    assert "fallbacks" not in kw and "betas" not in kw


def test_refusal_returns_none_and_is_counted(use_fake):
    use_fake("anthropic", lambda kind, kw: refusal_response())
    usage = llm.Usage()
    result = llm.parse("s", "u", Answer, usage=usage)
    assert result.parsed is None and result.refused and result.stop_details["category"] == "general_harms"
    assert usage.refusals == 1


def test_text_joins_text_blocks(use_fake):
    use_fake("anthropic", lambda kind, kw: text_response("  likely a late file  "))
    assert llm.text("s", "u").text == "likely a late file"


def test_api_errors_become_llm_unavailable(use_fake):
    import anthropic

    def boom(kind, kw):
        raise anthropic.APIConnectionError(request=None)
    use_fake("anthropic", boom)
    with pytest.raises(llm.LLMUnavailable):
        llm.parse("s", "u", Answer)
