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
    Python configuration for a Flink Java model provider.

    Implement :meth:`provider_identifier` and :meth:`to_options` to integrate a
    provider available in the Java classpath. Configuration does not install a
    provider or create a model.

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
        """Return the Java model provider factory identifier.

        This identifier is independent of the lookup name chosen at registration.

        .. versionadded:: 2.4.0
        """

    @abstractmethod
    def to_options(self) -> Dict[str, str]:
        """Return Java model options as string keys and values.

        .. versionadded:: 2.4.0
        """

    def model_option_key(self) -> str:
        """Return the Java option key for a model name, normally ``model``.

        Override this for providers whose model option has a different name.

        .. versionadded:: 2.4.0
        """
        return "model"


@PublicEvolving()
class OpenAIProvider(ModelProvider):
    """
    Configure Flink's ``openai`` provider for chat or embeddings.

    :param endpoint: Complete chat-completions or embeddings URL; forwarded unchanged.
    :param api_key: API key used to authenticate requests.
    :param model: Optional model name. It can be supplied when creating a model instead.
    :param system_prompt: System message. An empty string disables it.
    :param temperature: Sampling temperature.
    :param top_p: Probability cutoff for token selection.
    :param max_tokens: Maximum generated tokens.
    :param stop: Comma-separated stop sequences.
    :param presence_penalty: Token presence penalty between -2 and 2.
    :param n: Number of chat completion choices per input.
    :param seed: Sampling seed.
    :param response_format: ``text`` or ``json_object``.
    :param dimension: Embedding dimension.
    :param max_context_size: Maximum context tokens.
    :param context_overflow_action: Action when the context limit is exceeded.
    :param error_handling_strategy: ``RETRY``, ``FAILOVER``, or ``IGNORE``.
    :param retry_num: Number of request retries.
    :param retry_fallback_strategy: ``FAILOVER`` or ``IGNORE`` after retries are exhausted.

    Unspecified optional values are omitted so that Java supplies its defaults.

    Example::

        >>> provider = OpenAIProvider(
        ...     endpoint="https://api.openai.com/v1/chat/completions", api_key="key")
        >>> provider.provider_identifier()
        'openai'

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
        """Return ``openai``, the community Java factory identifier.

        .. versionadded:: 2.4.0
        """
        return "openai"

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of Java model options.

        .. versionadded:: 2.4.0
        """
        return dict(self._options)


@PublicEvolving()
class TritonProvider(ModelProvider):
    """
    Configure Flink's ``triton`` provider for NVIDIA Triton Inference Server.

    Unspecified optional values are omitted so that Java supplies its defaults.
    Durations use Flink strings such as ``30 s`` and are parsed by Java.

    :param endpoint: Triton server URL; forwarded unchanged.
    :param model_name: Optional model name, which can be supplied when creating a model.
    :param model_version: Model version.
    :param timeout: HTTP request timeout.
    :param flatten_batch_dim: Flatten the batch dimension of array inputs.
    :param priority: Request priority between 0 and 255.
    :param sequence_id: Triton sequence identifier.
    :param sequence_start: Mark the start of a sequence.
    :param sequence_end: Mark the end of a sequence.
    :param compression: Request compression, currently ``gzip``.
    :param auth_token: Authentication token.
    :param custom_headers: Mapping of HTTP header names to string values.
    :param max_retries: Maximum additional attempts after a failed request.
    :param retry_initial_backoff: Initial retry delay.
    :param retry_max_backoff: Maximum retry delay.
    :param default_value: Raw fallback value, interpreted using the model's output type.
                          ``null`` means SQL NULL; omit the option to propagate failures.
    :param health_check_enabled: Enable server health checks.
    :param health_check_interval: Interval between health checks.
    :param circuit_breaker_enabled: Enable the circuit breaker.
    :param circuit_breaker_failure_threshold: Failure rate in (0, 1] that opens the breaker.
    :param circuit_breaker_timeout: Time to remain in the open state.
    :param circuit_breaker_half_open_requests: Successful probes needed to close the breaker.

    Example::

        >>> provider = TritonProvider(
        ...     "http://localhost:8000", model_name="image-model", max_retries=2)
        >>> provider.model_option_key()
        'model-name'

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
        """Return ``triton``, the community Java factory identifier.

        .. versionadded:: 2.4.0
        """
        return "triton"

    def model_option_key(self) -> str:
        """Return ``model-name``, the Triton Java option for a model name.

        .. versionadded:: 2.4.0
        """
        return "model-name"

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of Java model options.

        .. versionadded:: 2.4.0
        """
        return dict(self._options)


@PublicEvolving()
class GenericProvider(ModelProvider):
    """
    Configure an installed Java provider with raw string options.

    Option keys and values are forwarded unchanged. Use a typed provider or a
    :class:`ModelProvider` subclass for Python-style constructor arguments.

    :param identifier: Java model provider factory identifier.
    :param options: Raw Java option names and string values.

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
        """Return the configured Java factory identifier.

        .. versionadded:: 2.4.0
        """
        return self._identifier

    def to_options(self) -> Dict[str, str]:
        """Return a fresh dictionary of the unchanged Java options.

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
    Set a process-global provider under an explicit or derived lookup name.

    Pass only a provider to use its :meth:`ModelProvider.provider_identifier` as
    the lookup name. A named registration does not inspect the provider. Neither
    form serializes the provider or accesses Java.

    Setting an existing name replaces its configuration without changing its list
    position or the default name. A default bound to that name selects the replacement.
    Registrations survive clearing or replacing the TableEnvironment.

    :param name: Explicit lookup name, independent of the Java factory identifier.
                 Omit it when supplying only a provider.
    :param provider: Provider configuration, passed alone or with a lookup name.
    :raises TypeError: If the provider is not a ModelProvider or the lookup name
                       (explicit or derived) is not a string.
    :raises ValueError: If the lookup name is empty or whitespace.

    Example::

        >>> import pyflink.dataframe as pf
        >>> provider = pf.GenericProvider("my-provider")
        >>> pf.set_model_provider(provider)
        >>> pf.set_model_provider(provider=provider)
        >>> pf.set_model_provider(name="custom", provider=provider)

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
    Return registered lookup names in first-registration order.

    Replacing a configuration preserves its name's position. The returned list
    can be modified without affecting registrations.

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
    Choose a registered provider as the process-global default.

    A sole registered provider is selected automatically. With multiple providers,
    explicitly select a default or a provider for each operation. An explicit default
    remains selected when additional providers are registered. Replacing the configuration
    at the default name makes subsequent selection use the replacement provider.

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
