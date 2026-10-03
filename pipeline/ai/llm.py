"""One small wrapper for every Claude call in the pipeline.

- Provider is configuration, not code: LLM_PROVIDER = none | anthropic | bedrock.
  `none` (the default) makes every LLM step skip cleanly, so pipelines, CI and
  tests never need a key.
- Structured outputs: parse() validates the response against a Pydantic model
  (messages.parse), so callers get typed objects, never free-form JSON to scrape.
- Refusals: a declined request returns None instead of raising, and on the
  Claude API the server-side fallback ("default") lets another Claude model
  answer first. Bedrock doesn't take that parameter, so there a refusal simply
  routes the item back to the rule-based path / human review.
- Cost: low effort by default (short, well-specified tasks), small max_tokens,
  and token usage returned to callers so runs can log what they spent.
- Errors: API/network failures raise LLMUnavailable; every caller treats the
  LLM as optional and degrades to its non-LLM behavior.
"""
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TypeVar

from pydantic import BaseModel

from pipeline import config

T = TypeVar("T", bound=BaseModel)

FALLBACK_BETA = "server-side-fallback-2026-07-01"


class LLMUnavailable(Exception):
    """The LLM step can't run (disabled, misconfigured, or the API failed)."""


@dataclass
class Usage:
    # Running token totals for a pipeline step (for logging / cost tracking).
    requests: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    refusals: int = 0

    def add(self, response) -> None:
        self.requests += 1
        u = getattr(response, "usage", None)
        if u is not None:
            self.input_tokens += getattr(u, "input_tokens", 0) or 0
            self.output_tokens += getattr(u, "output_tokens", 0) or 0


@dataclass
class Result:
    parsed: object | None          # the validated Pydantic object (None on refusal)
    text: str | None = None        # plain-text answer (text() calls)
    refused: bool = False
    stop_details: dict = field(default_factory=dict)


def enabled() -> bool:
    return config.LLM_PROVIDER in ("anthropic", "bedrock")


def model_id() -> str:
    # Bedrock model ids carry an "anthropic." prefix; the Claude API uses the bare id.
    if config.LLM_PROVIDER == "bedrock" and not config.LLM_MODEL.startswith("anthropic."):
        return f"anthropic.{config.LLM_MODEL}"
    return config.LLM_MODEL


@lru_cache(maxsize=1)
def client():
    """The SDK client for the configured provider (built once per process)."""
    import anthropic  # imported lazily: the pipeline runs fine without an LLM configured
    if config.LLM_PROVIDER == "anthropic":
        return anthropic.Anthropic()        # ANTHROPIC_API_KEY or an `ant auth login` profile
    if config.LLM_PROVIDER == "bedrock":
        return anthropic.AnthropicBedrockMantle(aws_region=config.AWS_REGION)
    raise LLMUnavailable(f"LLM_PROVIDER={config.LLM_PROVIDER!r}: LLM steps are disabled")


def _provider_kwargs() -> dict:
    # Server-side refusal fallback is a Claude API feature; Bedrock rejects the parameter.
    if config.LLM_PROVIDER == "anthropic":
        return {"betas": [FALLBACK_BETA], "fallbacks": "default"}
    return {}


def _refused(response) -> tuple[bool, dict]:
    if getattr(response, "stop_reason", None) != "refusal":
        return False, {}
    details = getattr(response, "stop_details", None)
    return True, {"category": getattr(details, "category", None),
                  "explanation": getattr(details, "explanation", None)}


def parse(system: str, user: str, schema: type[T], usage: Usage | None = None,
          max_tokens: int = 4000) -> Result:
    """One structured-output call: returns Result.parsed as a validated `schema` instance."""
    if not enabled():
        raise LLMUnavailable("LLM_PROVIDER is none")
    import anthropic
    try:
        response = client().beta.messages.parse(
            model=model_id(),
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
            output_format=schema,
            output_config={"effort": config.LLM_EFFORT},
            **_provider_kwargs(),
        )
    except anthropic.APIError as exc:          # 4xx/5xx after SDK retries, or network failure
        raise LLMUnavailable(f"{type(exc).__name__}: {exc}") from exc
    if usage is not None:
        usage.add(response)
    refused, details = _refused(response)
    if refused:
        if usage is not None:
            usage.refusals += 1
        return Result(parsed=None, refused=True, stop_details=details)
    return Result(parsed=response.parsed_output)


def text(system: str, user: str, usage: Usage | None = None, max_tokens: int = 2000) -> Result:
    """One plain-text call (e.g. a short explanation)."""
    if not enabled():
        raise LLMUnavailable("LLM_PROVIDER is none")
    import anthropic
    try:
        response = client().beta.messages.create(
            model=model_id(),
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
            output_config={"effort": config.LLM_EFFORT},
            **_provider_kwargs(),
        )
    except anthropic.APIError as exc:
        raise LLMUnavailable(f"{type(exc).__name__}: {exc}") from exc
    if usage is not None:
        usage.add(response)
    refused, details = _refused(response)
    if refused:
        if usage is not None:
            usage.refusals += 1
        return Result(parsed=None, refused=True, stop_details=details)
    answer = "".join(b.text for b in response.content if getattr(b, "type", None) == "text").strip()
    return Result(parsed=None, text=answer or None)
