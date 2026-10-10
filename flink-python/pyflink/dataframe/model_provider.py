################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from abc import ABC, abstractmethod
from collections.abc import Mapping as MappingABC
import json
from typing import Dict, List, Literal, Mapping, Optional, Tuple, overload

from pyflink.dataframe.validation import _require_int, _require_number
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "ModelProvider", "OpenAIProvider", "TritonProvider", "GenericProvider",
    "set_model_provider", "set_default_model_provider", "list_model_providers",
]


def _validate_name(name: object, argument: str) -> str:
    if not isinstance(name, str):
        raise TypeError(f"{argument} must be a string")
    if not name.strip():
        raise ValueError(f"{argument} must not be empty or whitespace")
    return name


def _stringify_options(options: Dict[str, object]) -> Dict[str, str]:
    return {
        name: str(value).lower() if isinstance(value, bool) else str(value)
        for name, value in options.items() if value is not None
    }


def _validate_type(name: str, value: object, expected: type) -> None:
    if value is not None and not isinstance(value, expected):
        raise TypeError(f"{name} must be a {expected.__name__}")


def _enum_option(name: str, value: Optional[str], choices: Tuple[str, ...]) -> Optional[str]:
    _validate_type(name, value, str)
    if value is None:
        return None
    for choice in choices:
        if value.lower() == choice.lower():
            return choice
    raise ValueError(f"{name} must be one of {choices}")


def _header_option(headers: Optional[Mapping[str, str]]) -> Optional[str]:
    if headers is None:
        return None
    if not isinstance(headers, MappingABC):
        raise TypeError("custom_headers must be a mapping of strings to strings")
    if any(not isinstance(key, str) or not isinstance(value, str)
           for key, value in headers.items()):
        raise TypeError("custom_headers must contain string keys and values")
    return json.dumps(dict(headers))


@PublicEvolving()
class ModelProvider(ABC):
    """
    Base class for model provider configurations.

    Subclass this to configure a provider available in your application's classpath.
    Implement :meth:`provider_identifier` to identify the provider and :meth:`to_options`
    to supply its options as string keys and values.

    Example::

        >>> class MyProvider(ModelProvider):
        ...     def provider_identifier(self):
        ...         return "my-provider"
        ...     def to_options(self):
        ...         return {"endpoint": "https://example.test/inference"}
        >>> MyProvider().provider_identifier()
        'my-provider'

    .. versionadded:: 2.4.0
    """

    @abstractmethod
    def provider_identifier(self) -> str:
        """Return the provider identifier, such as ``openai`` or ``triton``.

        .. versionadded:: 2.4.0
        """

    @abstractmethod
    def to_options(self) -> Dict[str, str]:
        """Return provider options as string keys and values.

        .. versionadded:: 2.4.0
        """

    def model_option_key(self) -> str:
        """Return the option key for a model name, normally ``model``.

        Override this for providers whose model option has a different name.

        .. versionadded:: 2.4.0
        """
        return "model"


@PublicEvolving()
class OpenAIProvider(ModelProvider):
    """
    Configure an OpenAI-compatible service for chat completions or embeddings.

    The full endpoint URL selects the task: use a URL ending in ``/chat/completions``
    for chat or ``/embeddings`` for embedding vectors. Chat options such as
    ``system_prompt`` and ``temperature`` apply to chat requests; ``dimension`` applies
    to embeddings. Supply a model name here or when creating a model.

    All optional parameters default to ``None``. Omitted options use the Flink defaults
    described below, or the service's defaults where Flink defines none.

    :param endpoint: Required full API URL, for example
                     ``https://api.openai.com/v1/chat/completions``.
    :param api_key: Required API key for authenticating requests.
    :param model: Model name to use. Default: ``None``; if omitted, supply it when
                  creating a model.
    :param system_prompt: System message that guides the chat response. If omitted,
                          Flink uses ``"You are a helpful assistant."``. Set ``""``
                          for an empty system message.
    :param temperature: Controls randomness in chat responses; typical values are
                        between 0.0 and 1.0. Default: ``None`` (service default).
    :param top_p: Probability cutoff for token selection, between 0 and 1. Usually
                  set either this or ``temperature``. Default: ``None`` (service default).
    :param max_tokens: Maximum number of tokens generated in a chat completion.
                       Default: ``None`` (service default).
    :param stop: Comma-separated strings that stop generation when encountered.
                 Default: ``None`` (no stop sequences supplied).
    :param presence_penalty: Value between -2 and 2. Positive values discourage tokens
                             already used in the response, encouraging new topics.
                             Default: ``None`` (service default).
    :param n: Number of chat completion choices generated per input. Setting this to
              1 limits token usage. Default: ``None`` (service default).
    :param seed: Seed for best-effort repeatable sampling. Default: ``None`` (no seed
                 supplied).
    :param response_format: Chat response format: ``text`` or ``json_object``.
                            Default: ``None`` (service default).
    :param dimension: Number of elements in each embedding vector. Default: ``None``
                      (the embedding model's default dimension).
    :param max_context_size: Maximum number of input context tokens before applying
                             ``context_overflow_action``. Default: ``None`` (no Flink
                             context limit).
    :param context_overflow_action: ``truncated-tail`` removes excess tokens from the
                                   end, ``truncated-head`` removes them from the start,
                                   and ``skipped`` skips the input. Each has a ``-log``
                                   variant that logs the action. If omitted, Flink uses
                                   ``truncated-tail``.
    :param error_handling_strategy: ``RETRY`` retries failed requests, ``FAILOVER``
                                   fails the job, and ``IGNORE`` skips the failed input.
                                   If omitted, Flink uses ``RETRY``.
    :param retry_num: Number of retries when ``error_handling_strategy`` is ``RETRY``.
                      If omitted, Flink uses ``100``.
    :param retry_fallback_strategy: Action after retries are exhausted: ``FAILOVER``
                                   fails the job and ``IGNORE`` skips the failed input.
                                   If omitted, Flink uses ``FAILOVER``.

    Example::

        >>> import pyflink.dataframe as pf
        >>> chat = pf.OpenAIProvider(
        ...     endpoint="https://api.openai.com/v1/chat/completions", api_key="key")
        >>> pf.set_model_provider("chat", chat)
        >>> embeddings = pf.OpenAIProvider(
        ...     endpoint="https://api.openai.com/v1/embeddings", api_key="key")
        >>> pf.set_model_provider("embed", embeddings)

    .. versionadded:: 2.4.0
    """

    def __init__(
        self, endpoint: str, api_key: str, *, model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stop: Optional[str] = None,
        presence_penalty: Optional[float] = None,
        n: Optional[int] = None,
        seed: Optional[int] = None,
        response_format: Optional[Literal["text", "json_object"]] = None,
        dimension: Optional[int] = None,
        max_context_size: Optional[int] = None,
        context_overflow_action: Optional[Literal[
            "truncated-tail", "truncated-tail-log", "truncated-head", "truncated-head-log",
            "skipped", "skipped-log",
        ]] = None,
        error_handling_strategy: Optional[Literal["RETRY", "FAILOVER", "IGNORE"]] = None,
        retry_num: Optional[int] = None,
        retry_fallback_strategy: Optional[Literal["FAILOVER", "IGNORE"]] = None,
    ):
        _validate_name(endpoint, "endpoint")
        _validate_name(api_key, "api_key")
        options: Dict[str, object] = {
            "endpoint": endpoint, "api-key": api_key, "model": model,
            "system-prompt": system_prompt, "temperature": temperature, "top-p": top_p,
            "max-tokens": max_tokens, "stop": stop, "presence-penalty": presence_penalty,
            "n": n, "seed": seed, "dimension": dimension, "max-context-size": max_context_size,
            "retry-num": retry_num,
            "response-format": _enum_option("response_format", response_format,
                                            ("text", "json_object")),
            "context-overflow-action": _enum_option(
                "context_overflow_action", context_overflow_action,
                ("truncated-tail", "truncated-tail-log", "truncated-head", "truncated-head-log",
                 "skipped", "skipped-log")),
            "error-handling-strategy": _enum_option(
                "error_handling_strategy", error_handling_strategy,
                ("RETRY", "FAILOVER", "IGNORE")),
            "retry-fallback-strategy": _enum_option(
                "retry_fallback_strategy", retry_fallback_strategy, ("FAILOVER", "IGNORE")),
        }
        for name in ("model", "system-prompt", "stop"):
            _validate_type(name, options[name], str)
        for name, integer, minimum in (
            ("max_tokens", max_tokens, 1), ("n", n, 1), ("dimension", dimension, 1),
            ("max_context_size", max_context_size, 1), ("retry_num", retry_num, 0),
            ("seed", seed, None),
        ):
            if integer is not None:
                _require_int(integer, name, minimum)
        for name, number, lower_bound, upper_bound in (
            ("temperature", temperature, None, None), ("top_p", top_p, 0, 1),
            ("presence_penalty", presence_penalty, -2, 2),
        ):
            if number is not None:
                _require_number(number, name, lower_bound, maximum=upper_bound)
        self._options = _stringify_options(options)

    def provider_identifier(self) -> str:
        """Return ``openai``.

        .. versionadded:: 2.4.0
        """
        return "openai"

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of provider options.

        .. versionadded:: 2.4.0
        """
        return dict(self._options)


@PublicEvolving()
class TritonProvider(ModelProvider):
    """
    Configure inference requests to a model served by NVIDIA Triton Inference Server.

    Supply the server URL and a model name here or when creating a model. For array
    inputs, use ``flatten_batch_dim`` to match the model's expected shape. Retry and
    fallback options control how failed requests are handled; health checks and the
    circuit breaker can reduce requests to an unavailable server.

    All optional parameters default to ``None``. Omitted options use the Flink defaults
    described below. Specify durations as strings such as ``"30 s"`` or ``"100 ms"``.

    :param endpoint: Required Triton server URL, for example ``http://localhost:8000``.
    :param model_name: Name of the model to invoke. Default: ``None``; if omitted,
                       supply it when creating a model.
    :param model_version: Model version to invoke. If omitted, Flink uses ``"latest"``.
    :param timeout: HTTP timeout for each request, separate from Flink's asynchronous
                    prediction timeout. If omitted, Flink uses ``"30 s"``.
    :param flatten_batch_dim: Convert the array input shape from ``[1, N]`` to ``[N]``
                              when the model expects a vector without a batch dimension.
                              If omitted, Flink uses ``False``.
    :param priority: Request priority between 0 and 255. Default: ``None`` (no priority
                     supplied).
    :param sequence_id: Identifier shared by requests in a stateful model sequence.
                        Default: ``None`` (no sequence identifier supplied).
    :param sequence_start: Mark requests as starting a stateful sequence. If omitted,
                           Flink uses ``False``.
    :param sequence_end: Mark requests as ending a stateful sequence. If omitted,
                         Flink uses ``False``.
    :param compression: Compress request bodies using ``gzip``. Default: ``None``
                        (no compression).
    :param auth_token: Authentication token sent as a Bearer token. Default: ``None``
                       (no token supplied).
    :param custom_headers: Additional HTTP headers as a mapping, for example
                           ``{"X-Trace-Id": "abc"}``. Default: ``None`` (no extra headers).
    :param max_retries: Additional attempts for transient failures, such as network
                        errors and server errors. If omitted, Flink uses ``0`` (no retries).
    :param retry_initial_backoff: Initial delay between retries; delays increase
                                  exponentially up to ``retry_max_backoff``. If omitted,
                                  Flink uses ``"100 ms"``.
    :param retry_max_backoff: Maximum delay between retries. If omitted, Flink uses
                              ``"30 s"``.
    :param default_value: Fallback value when inference fails, expressed as a string
                          matching the output type: plain text for strings, a numeric
                          string for numbers, a JSON array for arrays, or ``"null"`` for
                          SQL NULL. Default: ``None`` (propagate failures).
    :param health_check_enabled: Enable periodic server health checks. If omitted,
                                 Flink uses ``False``.
    :param health_check_interval: Time between health checks when enabled. If omitted,
                                  Flink uses ``"30 s"``.
    :param circuit_breaker_enabled: Temporarily stop sending requests when the server
                                    has a high failure rate. If omitted, Flink uses ``False``.
    :param circuit_breaker_failure_threshold: Failure rate in ``(0, 1]`` that opens the
                                              circuit breaker; ``0.5`` means 50% failures.
                                              If omitted, Flink uses ``0.5``.
    :param circuit_breaker_timeout: Time to wait before probing recovery after the
                                    circuit breaker opens. If omitted, Flink uses ``"60 s"``.
    :param circuit_breaker_half_open_requests: Successful recovery probes needed to
                                              close the circuit breaker. If omitted,
                                              Flink uses ``3``.

    Example::

        >>> import pyflink.dataframe as pf
        >>> provider = pf.TritonProvider(
        ...     endpoint="http://localhost:8000", model_name="classifier",
        ...     flatten_batch_dim=True, max_retries=2, default_value="-1")
        >>> pf.set_model_provider("classifier", provider)

    .. versionadded:: 2.4.0
    """

    def __init__(
        self, endpoint: str, *, model_name: Optional[str] = None,
        model_version: Optional[str] = None,
        timeout: Optional[str] = None,
        flatten_batch_dim: Optional[bool] = None,
        priority: Optional[int] = None,
        sequence_id: Optional[str] = None,
        sequence_start: Optional[bool] = None,
        sequence_end: Optional[bool] = None,
        compression: Optional[Literal["gzip"]] = None,
        auth_token: Optional[str] = None,
        custom_headers: Optional[Mapping[str, str]] = None,
        max_retries: Optional[int] = None,
        retry_initial_backoff: Optional[str] = None,
        retry_max_backoff: Optional[str] = None,
        default_value: Optional[str] = None,
        health_check_enabled: Optional[bool] = None,
        health_check_interval: Optional[str] = None,
        circuit_breaker_enabled: Optional[bool] = None,
        circuit_breaker_failure_threshold: Optional[float] = None,
        circuit_breaker_timeout: Optional[str] = None,
        circuit_breaker_half_open_requests: Optional[int] = None,
    ):
        _validate_name(endpoint, "endpoint")
        options: Dict[str, object] = {
            "endpoint": endpoint, "model-name": model_name, "model-version": model_version,
            "timeout": timeout, "flatten-batch-dim": flatten_batch_dim, "priority": priority,
            "sequence-id": sequence_id, "sequence-start": sequence_start,
            "sequence-end": sequence_end,
            "compression": _enum_option("compression", compression, ("gzip",)),
            "auth-token": auth_token, "custom-headers": _header_option(custom_headers),
            "max-retries": max_retries, "retry-initial-backoff": retry_initial_backoff,
            "retry-max-backoff": retry_max_backoff, "default-value": default_value,
            "health-check-enabled": health_check_enabled,
            "health-check-interval": health_check_interval,
            "circuit-breaker-enabled": circuit_breaker_enabled,
            "circuit-breaker-failure-threshold": circuit_breaker_failure_threshold,
            "circuit-breaker-timeout": circuit_breaker_timeout,
            "circuit-breaker-half-open-requests": circuit_breaker_half_open_requests,
        }
        for name in ("model-name", "model-version", "timeout", "sequence-id", "auth-token",
                     "retry-initial-backoff", "retry-max-backoff", "default-value",
                     "health-check-interval", "circuit-breaker-timeout"):
            _validate_type(name, options[name], str)
        for name in ("flatten-batch-dim", "sequence-start", "sequence-end", "health-check-enabled",
                     "circuit-breaker-enabled"):
            _validate_type(name, options[name], bool)
        for name, integer, minimum, maximum in (
            ("priority", priority, 0, 255), ("max_retries", max_retries, 0, None),
            ("circuit_breaker_half_open_requests", circuit_breaker_half_open_requests, 1, None),
        ):
            if integer is not None:
                _require_int(integer, name, minimum, maximum=maximum)
        if circuit_breaker_failure_threshold is not None:
            _require_number(circuit_breaker_failure_threshold, "circuit_breaker_failure_threshold",
                            0, maximum=1, include_minimum=False)
        self._options = _stringify_options(options)

    def provider_identifier(self) -> str:
        """Return ``triton``.

        .. versionadded:: 2.4.0
        """
        return "triton"

    def model_option_key(self) -> str:
        """Return ``model-name``, the option key for a model name.

        .. versionadded:: 2.4.0
        """
        return "model-name"

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of provider options.

        .. versionadded:: 2.4.0
        """
        return dict(self._options)


@PublicEvolving()
class GenericProvider(ModelProvider):
    """
    Configure an installed model provider with its option names and string values.

    Use this for a provider without a dedicated Python configuration class. Option
    names and values follow that provider's documentation.

    :param identifier: Java model provider factory identifier.
    :param options: Provider option names and string values.

    Example::

        >>> provider = GenericProvider(
        ...     "my-provider", **{"endpoint": "https://example.test/inference"})
        >>> provider.to_options()
        {'endpoint': 'https://example.test/inference'}

    .. versionadded:: 2.4.0
    """

    def __init__(self, identifier: str, /, **options: str):
        _validate_name(identifier, "identifier")
        for key, value in options.items():
            if not isinstance(value, str):
                raise TypeError(f"Option {key!r} must have a string value")
        self._identifier = identifier
        self._options = dict(options)

    def provider_identifier(self) -> str:
        """Return the configured provider identifier.

        .. versionadded:: 2.4.0
        """
        return self._identifier

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of provider options.

        .. versionadded:: 2.4.0
        """
        return dict(self._options)


_provider_registry: Dict[str, ModelProvider] = {}
_default_provider: Optional[str] = None
_UNSET = object()


@overload
def set_model_provider(provider: ModelProvider, /) -> None:
    ...


@overload
def set_model_provider(*, provider: ModelProvider) -> None:
    ...


@overload
def set_model_provider(name: str, provider: ModelProvider) -> None:
    ...


@PublicEvolving()
def set_model_provider(
    *args: object, name: object = _UNSET, provider: object = _UNSET,
) -> None:
    """
    Register a model provider configuration.

    Pass a provider alone to register it under its identifier, such as ``openai``.
    Supply a name to keep several configurations of the same provider.

    Registering an existing name updates its configuration. Registered providers are
    shared across DataFrame environments in the current Python process.

    :param name: Name used to look up the provider. Omit it to use the provider identifier.
    :param provider: Provider configuration to register.

    Example::

        >>> import pyflink.dataframe as pf
        >>> provider = pf.OpenAIProvider(
        ...     "https://api.openai.com/v1/chat/completions", api_key="key")
        >>> pf.set_model_provider(provider)
        >>> pf.set_model_provider("chat", provider)

    .. versionadded:: 2.4.0
    """
    if len(args) > 2:
        raise TypeError("set_model_provider() takes at most 2 positional arguments")
    if args:
        if name is not _UNSET:
            raise TypeError("set_model_provider() got multiple values for argument 'name'")
        if len(args) == 2:
            if provider is not _UNSET:
                raise TypeError("set_model_provider() got multiple values for argument 'provider'")
            name, provider = args
        elif provider is _UNSET:
            provider = args[0]
        else:
            name = args[0]
    if not isinstance(provider, ModelProvider):
        raise TypeError("provider must be a ModelProvider")
    if name is _UNSET:
        name = provider.provider_identifier()
    name = _validate_name(name, "name")
    _provider_registry[name] = provider


@PublicEvolving()
def list_model_providers() -> List[str]:
    """
    Return registered provider names in registration order.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_model_provider("custom", pf.GenericProvider("my-provider"))
        >>> pf.list_model_providers()
        ['custom']

    .. versionadded:: 2.4.0
    """
    return list(_provider_registry)


@PublicEvolving()
def set_default_model_provider(name: str) -> None:
    """
    Select the registered provider to use by default.

    A single registered provider is selected automatically. If you register several
    providers, select a default or choose a provider for each operation.

    :param name: An already registered provider lookup name.
    :raises ValueError: If the name is not registered.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.set_model_provider("custom", pf.GenericProvider("my-provider"))
        >>> pf.set_default_model_provider("custom")

    .. versionadded:: 2.4.0
    """
    global _default_provider
    _validate_name(name, "name")
    if name not in _provider_registry:
        raise ValueError(f"Model provider {name!r} is not registered")
    _default_provider = name


def _resolve_provider(name: Optional[str] = None) -> ModelProvider:
    if name is not None:
        _validate_name(name, "name")
        if name not in _provider_registry:
            raise ValueError(f"Model provider {name!r} is not registered")
        return _provider_registry[name]
    if _default_provider is not None:
        return _provider_registry[_default_provider]
    if len(_provider_registry) == 1:
        return next(iter(_provider_registry.values()))
    if not _provider_registry:
        raise ValueError("No model provider is registered")
    raise ValueError(
        "Multiple model providers are registered; call set_default_model_provider() "
        "or select a provider explicitly"
    )
