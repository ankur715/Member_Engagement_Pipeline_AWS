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


def test_full_bedrock_ids_are_used_verbatim(use_fake, monkeypatch):
    monkeypatch.setattr(config, "LLM_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
    fake = use_fake("bedrock", lambda kind, kw: parsed_response(Answer(value="ok")))
    llm.parse("s", "u", Answer)
    _, kw = fake.messages.calls[0]
    assert kw["model"] == "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    assert "output_config" not in kw                      # Haiku 4.5 doesn't take effort


@pytest.mark.parametrize("model,tag", [
    ("claude-opus-5-5", "claude-opus-5-5"),
    ("us.anthropic.claude-haiku-4-5-20251001-v1:0", "claude-haiku-4-5"),
    ("global.anthropic.claude-sonnet-5-5", "claude-sonnet-5-5"),
    ("anthropic.claude-opus-5-5", "claude-opus-5-5"),
])
def test_model_tag(monkeypatch, model, tag):
    monkeypatch.setattr(config, "LLM_MODEL", model)
    assert llm.model_tag() == tag


def test_bedrock_endpoint_selects_client(monkeypatch):
    import anthropic
    monkeypatch.setattr(config, "LLM_PROVIDER", "bedrock")
    made = {}
    monkeypatch.setattr(anthropic, "AnthropicBedrock", lambda **kw: made.setdefault("runtime", kw))
    monkeypatch.setattr(anthropic, "AnthropicBedrockMantle", lambda **kw: made.setdefault("mantle", kw))
    for endpoint in ("runtime", "mantle"):
        monkeypatch.setattr(config, "LLM_BEDROCK_ENDPOINT", endpoint)
        llm.client.cache_clear()
        llm.client()
    llm.client.cache_clear()
    assert made == {"runtime": {"aws_region": "us-east-1"}, "mantle": {"aws_region": "us-east-1"}}
