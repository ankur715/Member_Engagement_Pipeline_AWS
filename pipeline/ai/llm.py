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
import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TypeVar

from pydantic import BaseModel

from pipeline import config

# T = "some Pydantic model class": lets parse() promise it returns that same type.
T = TypeVar("T", bound=BaseModel)

# Beta flag that turns on server-side fallbacks (Claude API only, not Bedrock).
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
        # Called once per API response; every response reports its own token counts.
        self.requests += 1
        u = getattr(response, "usage", None)  # getattr: test fakes may not have .usage
        if u is not None:
            self.input_tokens += getattr(u, "input_tokens", 0) or 0
            self.output_tokens += getattr(u, "output_tokens", 0) or 0


@dataclass
class Result:
    parsed: object | None          # the validated Pydantic object (None on refusal)
    text: str | None = None        # plain-text answer (text() calls)
    refused: bool = False          # True when the model declined the request
    stop_details: dict = field(default_factory=dict)   # why it declined (category/explanation)


def enabled() -> bool:
    # Every LLM step checks this first and skips when the provider is "none".
    return config.LLM_PROVIDER in ("anthropic", "bedrock")


def model_id() -> str:
    # A value with a dot is already a full Bedrock model id or inference profile
    # (anthropic.claude-..., us.anthropic.claude-...): use it verbatim. Otherwise
    # Bedrock needs the "anthropic." prefix; the Claude API uses the bare id.
    if config.LLM_PROVIDER == "bedrock" and "." not in config.LLM_MODEL:
        return f"anthropic.{config.LLM_MODEL}"
    return config.LLM_MODEL


def model_tag() -> str:
    """Short, stable model name for labels: us.anthropic.claude-haiku-4-5-20251001-v1:0 -> claude-haiku-4-5."""
    tag = re.sub(r"^(us|eu|apac|global)\.", "", config.LLM_MODEL)  # drop the inference-profile region
    tag = re.sub(r"^anthropic\.", "", tag)                          # drop the Bedrock vendor prefix
    tag = re.sub(r"-v\d+(:\d+)?$", "", tag)                         # drop the Bedrock version suffix
    return re.sub(r"-\d{8}$", "", tag)                              # drop the release date


def _supports_effort() -> bool:
    # The effort setting works on current models; Claude Haiku 4.5 rejects it.
    return bool(config.LLM_EFFORT) and "haiku-4-5" not in config.LLM_MODEL


def _sdk():
    # Imported lazily: the pipeline runs fine without an LLM configured. A missing
    # SDK (e.g. an environment built before it was added) is "unavailable", so
    # optional LLM steps skip instead of crashing the task.
    try:
        import anthropic
    except ImportError as exc:
        raise LLMUnavailable(f"the anthropic SDK isn't installed in this environment ({exc})") from exc
    return anthropic


@lru_cache(maxsize=1)  # one client per process; it holds the HTTP connection pool
def client():
    """The SDK client for the configured provider (built once per process)."""
    anthropic = _sdk()
    if config.LLM_PROVIDER == "anthropic":
        return anthropic.Anthropic()        # ANTHROPIC_API_KEY or an `ant auth login` profile
    if config.LLM_PROVIDER == "bedrock" and config.LLM_BEDROCK_ENDPOINT == "runtime":
        return anthropic.AnthropicBedrock(aws_region=config.AWS_REGION)   # bedrock-runtime InvokeModel
    if config.LLM_PROVIDER == "bedrock":
        return anthropic.AnthropicBedrockMantle(aws_region=config.AWS_REGION)   # Bedrock Messages API endpoint
    raise LLMUnavailable(f"LLM_PROVIDER={config.LLM_PROVIDER!r}: LLM steps are disabled")


def _provider_kwargs() -> dict:
    # Extra request settings that depend on the provider and model.
    kwargs = {}
    if _supports_effort():
        kwargs["output_config"] = {"effort": config.LLM_EFFORT}   # "low" = fewer tokens, cheaper
    # Server-side refusal fallback is a Claude API feature; Bedrock rejects the parameter.
    if config.LLM_PROVIDER == "anthropic":
        kwargs.update(betas=[FALLBACK_BETA], fallbacks="default")
    return kwargs


def _refused(response) -> tuple[bool, dict]:
    # A refusal is a normal response with stop_reason "refusal", not an exception.
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
    anthropic = _sdk()
    try:
        response = client().beta.messages.parse(
            model=model_id(),
            max_tokens=max_tokens,           # hard cap on output length (and cost)
            system=system,                   # the instructions: task, rules, output format
            messages=[{"role": "user", "content": user}],   # the data for this call
            output_format=schema,            # the answer must match this Pydantic model
            **_provider_kwargs(),
        )
    except anthropic.APIError as exc:          # 4xx/5xx after SDK retries, or network failure
        raise LLMUnavailable(f"{type(exc).__name__}: {exc}") from exc
    if usage is not None:
        usage.add(response)              # count tokens even for refusals (they still cost)
    refused, details = _refused(response)
    if refused:
        if usage is not None:
            usage.refusals += 1
        return Result(parsed=None, refused=True, stop_details=details)
    return Result(parsed=response.parsed_output)   # already validated by the SDK


def text(system: str, user: str, usage: Usage | None = None, max_tokens: int = 2000) -> Result:
    """One plain-text call (e.g. a short explanation)."""
    if not enabled():
        raise LLMUnavailable("LLM_PROVIDER is none")
    anthropic = _sdk()
    try:
        response = client().beta.messages.create(
            model=model_id(),
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
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
    # The response is a list of content blocks; join the text ones into one string.
    answer = "".join(b.text for b in response.content if getattr(b, "type", None) == "text").strip()
    return Result(parsed=None, text=answer or None)
