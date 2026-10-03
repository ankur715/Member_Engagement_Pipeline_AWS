"""A stand-in for the Anthropic SDK client so LLM code is tested offline.

It mimics just the surface pipeline.ai.llm uses -- client.beta.messages.parse /
.create -- and records every request so tests can assert on what was sent
(e.g. that PHI was redacted, effort/model/fallbacks were set).
"""
from types import SimpleNamespace


class FakeMessages:
    def __init__(self, responder):
        self.responder = responder        # fn(kind, kwargs) -> response-like object
        self.calls = []

    def parse(self, **kwargs):
        self.calls.append(("parse", kwargs))
        return self.responder("parse", kwargs)

    def create(self, **kwargs):
        self.calls.append(("create", kwargs))
        return self.responder("create", kwargs)


class FakeClient:
    def __init__(self, responder):
        self.messages = FakeMessages(responder)
        self.beta = SimpleNamespace(messages=self.messages)


def parsed_response(obj, input_tokens=100, output_tokens=20):
    return SimpleNamespace(stop_reason="end_turn", stop_details=None, parsed_output=obj,
                           usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens))


def text_response(text, input_tokens=100, output_tokens=40):
    return SimpleNamespace(stop_reason="end_turn", stop_details=None,
                           content=[SimpleNamespace(type="text", text=text)],
                           usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens))


def refusal_response():
    return SimpleNamespace(stop_reason="refusal",
                           stop_details=SimpleNamespace(category="general_harms", explanation="declined"),
                           parsed_output=None, content=[],
                           usage=SimpleNamespace(input_tokens=50, output_tokens=0))
