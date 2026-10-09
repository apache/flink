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

import json
import unittest
from inspect import signature
from typing import get_type_hints

import pyflink.dataframe as pf
from pyflink.dataframe import model_provider
from pyflink.table import TableEnvironment


class ModelProviderTests(unittest.TestCase):
    def test_triton_options_follow_community_option_names(self):
        self.assertEqual(pf.TritonProvider("http://localhost:8000").to_options(),
                         {"endpoint": "http://localhost:8000"})
        headers = {"X-Request": "comma, colon: quote'", "X-Empty": ""}
        provider = pf.TritonProvider(
            "http://localhost:8000", model_name="model", model_version="2", timeout="10 s",
            flatten_batch_dim=False, priority=0, sequence_id="18446744073709551615",
            sequence_start=False, sequence_end=True, compression="gzip", auth_token="token",
            custom_headers=headers, max_retries=0, retry_initial_backoff="100 ms",
            retry_max_backoff="1 s", default_value="null", health_check_enabled=True,
            health_check_interval="5 s", circuit_breaker_enabled=False,
            circuit_breaker_failure_threshold=0.5, circuit_breaker_timeout="30 s",
            circuit_breaker_half_open_requests=2,
        )
        self.assertEqual(provider.provider_identifier(), "triton")
        self.assertEqual(provider.model_option_key(), "model-name")
        options = provider.to_options()
        self.assertEqual(json.loads(options.pop("custom-headers")), headers)
        expected = {
            "endpoint": "http://localhost:8000", "model-name": "model", "model-version": "2",
            "timeout": "10 s", "flatten-batch-dim": "false", "priority": "0",
            "sequence-id": "18446744073709551615", "sequence-start": "false",
            "sequence-end": "true", "compression": "gzip", "auth-token": "token",
            "max-retries": "0", "retry-initial-backoff": "100 ms", "retry-max-backoff": "1 s",
            "default-value": "null", "health-check-enabled": "true", "health-check-interval": "5 s",
            "circuit-breaker-enabled": "false", "circuit-breaker-failure-threshold": "0.5",
            "circuit-breaker-timeout": "30 s", "circuit-breaker-half-open-requests": "2",
        }
        self.assertEqual(options, expected)
        headers["X-Request"] = "changed"
        self.assertEqual(json.loads(provider.to_options()["custom-headers"]),
                         {"X-Request": "comma, colon: quote'", "X-Empty": ""})
        provider.to_options().clear()
        self.assertEqual(set(provider.to_options()), set(expected) | {"custom-headers"})

    def test_triton_rejects_invalid_typed_options(self):
        for options, error in [
            ({"model_name": 1}, TypeError),
            ({"timeout": 10}, TypeError),
            ({"flatten_batch_dim": "false"}, TypeError),
            ({"sequence_start": 1}, TypeError),
            ({"priority": True}, TypeError),
            ({"priority": 256}, ValueError),
            ({"max_retries": -1}, ValueError),
            ({"compression": "deflate"}, ValueError),
            ({"default_value": [0.0]}, TypeError),
            ({"custom_headers": [("X-Header", "value")]}, TypeError),
            ({"custom_headers": {1: "value"}}, TypeError),
            ({"custom_headers": {"X-Header": 1}}, TypeError),
            ({"health_check_enabled": 1}, TypeError),
            ({"circuit_breaker_enabled": "false"}, TypeError),
            ({"circuit_breaker_failure_threshold": 0.0}, ValueError),
            ({"circuit_breaker_failure_threshold": float("inf")}, ValueError),
            ({"circuit_breaker_half_open_requests": 0}, ValueError),
            ({"retry_initial_backoff": 100}, TypeError),
            ({"extra_body": "{}"}, TypeError),
        ]:
            with self.subTest(options=options):
                with self.assertRaises(error):
                    pf.TritonProvider("http://localhost:8000", **options)

    def test_openai_connection_options_allow_deferred_model_selection(self):
        endpoint = "https://example.test/v1/embeddings"
        provider = pf.OpenAIProvider(endpoint=endpoint, api_key="key")
        self.assertEqual(provider.provider_identifier(), "openai")
        self.assertEqual(provider.model_option_key(), "model")
        self.assertEqual(provider.to_options(), {"endpoint": endpoint, "api-key": "key"})
        self.assertEqual(
            pf.OpenAIProvider(endpoint, "key", model="embed-model").to_options(),
            {"endpoint": endpoint, "api-key": "key", "model": "embed-model"},
        )

    def test_openai_options_follow_community_option_names(self):
        provider = pf.OpenAIProvider(
            "https://example.test/v1/chat/completions", "key", model="chat-model",
            system_prompt="", temperature=0.0, top_p=1.0, max_tokens=100,
            stop="END,STOP", presence_penalty=-1.0, n=2, seed=0,
            response_format="json_object", dimension=3, max_context_size=1000,
            context_overflow_action="truncated-head-log", error_handling_strategy="IGNORE",
            retry_num=0, retry_fallback_strategy="IGNORE",
        )
        expected = {
            "endpoint": "https://example.test/v1/chat/completions", "api-key": "key",
            "model": "chat-model", "system-prompt": "", "temperature": "0.0",
            "top-p": "1.0", "max-tokens": "100", "stop": "END,STOP",
            "presence-penalty": "-1.0", "n": "2", "seed": "0",
            "response-format": "json_object", "dimension": "3", "max-context-size": "1000",
            "context-overflow-action": "truncated-head-log", "error-handling-strategy": "IGNORE",
            "retry-num": "0", "retry-fallback-strategy": "IGNORE",
        }
        self.assertEqual(provider.to_options(), expected)
        provider.to_options().clear()
        self.assertEqual(provider.to_options(), expected)

    def test_openai_rejects_invalid_typed_options(self):
        for overrides, error in [
            ({"endpoint": None}, TypeError),
            ({"api_key": ""}, ValueError),
            ({"model": 1}, TypeError),
            ({"system_prompt": 1}, TypeError),
            ({"max_tokens": True}, TypeError),
            ({"dimension": "3"}, TypeError),
            ({"temperature": True}, TypeError),
            ({"temperature": float("nan")}, ValueError),
            ({"top_p": 1.1}, ValueError),
            ({"presence_penalty": -3.0}, ValueError),
            ({"retry_num": -1}, ValueError),
            ({"n": 0}, ValueError),
            ({"max_context_size": 0}, ValueError),
            ({"stop": ["STOP"]}, TypeError),
            ({"response_format": "json_schema"}, ValueError),
            ({"context_overflow_action": "unknown"}, ValueError),
            ({"error_handling_strategy": "unknown"}, ValueError),
            ({"retry_fallback_strategy": "RETRY"}, ValueError),
            ({"task": "chat/completions"}, TypeError),
        ]:
            with self.subTest(overrides=overrides):
                arguments = {"endpoint": "https://example.test/chat/completions", "api_key": "key"}
                arguments.update(overrides)
                with self.assertRaises(error):
                    pf.OpenAIProvider(**arguments)

    def test_generic_provider_preserves_raw_options(self):
        options = {"endpoint": "HTTPS://example.test/v1", "api-key": "key",
                   "custom_option": "", "enabled": "false", "identifier": "raw", "self": "raw"}
        provider = pf.GenericProvider("custom", **options)

        self.assertIsInstance(provider, pf.ModelProvider)
        self.assertEqual(provider.provider_identifier(), "custom")
        self.assertEqual(provider.model_option_key(), "model")
        self.assertEqual(provider.to_options(), options)
        provider.to_options().clear()
        self.assertEqual(provider.to_options(), options)

    def test_public_contracts_have_resolvable_type_hints(self):
        for api in (pf.set_model_provider, pf.set_default_model_provider, pf.list_model_providers,
                    pf.ModelProvider.provider_identifier, pf.ModelProvider.to_options,
                    pf.ModelProvider.model_option_key, pf.OpenAIProvider.__init__,
                    pf.TritonProvider.__init__, pf.GenericProvider.__init__):
            with self.subTest(api=api.__qualname__):
                self.assertTrue(get_type_hints(api))
        self.assertEqual(list(signature(pf.set_model_provider).parameters), ["name", "provider"])

    def test_generic_provider_rejects_invalid_configuration(self):
        for identifier, options, error in [
            (1, {}, TypeError),
            ("", {}, ValueError),
            ("  ", {}, ValueError),
            ("custom", {"enabled": True}, TypeError),
            ("custom", {"retries": 1}, TypeError),
            ("custom", {"endpoint": None}, TypeError),
        ]:
            with self.subTest(identifier=identifier, options=options):
                with self.assertRaises(error):
                    pf.GenericProvider(identifier, **options)


class ModelProviderRegistryTests(unittest.TestCase):
    def setUp(self):
        previous = dict(model_provider._provider_registry)
        self.addCleanup(model_provider._provider_registry.update, previous)
        self.addCleanup(model_provider._provider_registry.clear)
        model_provider._provider_registry.clear()
        previous_default = model_provider._default_provider
        self.addCleanup(setattr, model_provider, "_default_provider", previous_default)
        model_provider._default_provider = None

    def test_registration_is_lazy_and_names_are_ordered(self):
        class UnserializedProvider(pf.ModelProvider):
            def provider_identifier(self):
                raise AssertionError("Registration must not inspect the provider")

            def to_options(self):
                raise AssertionError("Registration must not serialize the provider")

        pf.set_model_provider("chat", UnserializedProvider())
        pf.set_model_provider("embed", pf.GenericProvider("custom"))

        names = pf.list_model_providers()
        self.assertEqual(names, ["chat", "embed"])
        names.clear()
        self.assertEqual(pf.list_model_providers(), ["chat", "embed"])

    def test_invalid_registration_preserves_registered_names(self):
        provider = pf.GenericProvider("custom")
        pf.set_model_provider("chat", provider)
        for name, value, error in [
            ("chat", pf.GenericProvider("replacement"), ValueError),
            ("", provider, ValueError),
            ("  ", provider, ValueError),
            (1, provider, TypeError),
            ("embed", None, TypeError),
            ("embed", "custom", TypeError),
        ]:
            with self.subTest(name=name, provider=value):
                with self.assertRaises(error):
                    pf.set_model_provider(name, value)
                self.assertEqual(pf.list_model_providers(), ["chat"])
                self.assertIs(model_provider._resolve_provider("chat"), provider)

    def test_selection_requires_choice_only_with_multiple_providers(self):
        chat = pf.GenericProvider("custom", task="chat")
        embed = pf.GenericProvider("custom", task="embed")
        with self.assertRaisesRegex(ValueError, "No model provider"):
            model_provider._resolve_provider()

        pf.set_model_provider("chat", chat)
        self.assertIs(model_provider._resolve_provider(), chat)
        pf.set_model_provider("embed", embed)
        with self.assertRaisesRegex(ValueError, "Multiple model providers"):
            model_provider._resolve_provider()

        pf.set_default_model_provider("chat")
        pf.set_model_provider("other", pf.GenericProvider("other"))
        self.assertIs(model_provider._resolve_provider(), chat)
        self.assertIs(model_provider._resolve_provider("embed"), embed)

    def test_invalid_selection_preserves_default(self):
        provider = pf.GenericProvider("custom")
        pf.set_model_provider("chat", provider)
        pf.set_default_model_provider("chat")
        for name, error in [("unknown", ValueError), ("", ValueError),
                            ("  ", ValueError), (1, TypeError), (None, TypeError)]:
            with self.subTest(name=name):
                with self.assertRaises(error):
                    pf.set_default_model_provider(name)
                self.assertIs(model_provider._resolve_provider(), provider)
                if name is not None:
                    with self.assertRaises(error):
                        model_provider._resolve_provider(name)

    def test_registry_and_default_survive_environment_replacement(self):
        previous_environment = pf.get_table_environment()
        buffered = dict(pf.config._buffered)
        self.addCleanup(pf.config._buffered.update, buffered)
        self.addCleanup(pf.config._buffered.clear)
        self.addCleanup(pf.set_table_environment, previous_environment)
        pf.config._buffered.clear()
        provider = pf.GenericProvider("custom")
        pf.set_model_provider("chat", provider)
        pf.set_model_provider("embed", pf.GenericProvider("custom"))
        pf.set_default_model_provider("chat")

        for environment in (object.__new__(TableEnvironment), None,
                            object.__new__(TableEnvironment)):
            pf.set_table_environment(environment)
            self.assertEqual(pf.list_model_providers(), ["chat", "embed"])
            self.assertIs(model_provider._resolve_provider(), provider)


if __name__ == '__main__':
    unittest.main()
