.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

===============
Model Providers
===============

Provider configurations expose Flink's Java model provider options through Python.
``OpenAIProvider`` supports OpenAI-compatible chat and embedding endpoints, while
``TritonProvider`` configures NVIDIA Triton Inference Server. Optional values are
omitted unless supplied, so the Java providers determine their defaults.

Pass a provider alone to register it under its Java factory identifier. The keyword
form ``set_model_provider(provider=provider)`` is equivalent. Supply an explicit
lookup name to keep several configurations of the same provider:

.. code-block:: python

    import pyflink.dataframe as pf

    pf.set_model_provider(pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/chat/completions", api_key="key"))
    pf.set_model_provider("embed", pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/embeddings", api_key="key"))
    pf.set_default_model_provider("openai")
    pf.list_model_providers()  # ["openai", "embed"]

Setting an existing name replaces its configuration. Both convenience and named
registration support updates, including repeated execution of a notebook cell.
The list keeps names in their first-registration order. Defaults are bound to
lookup names, so replacing the default configuration makes subsequent selection
use the new provider without changing the default name:

.. code-block:: python

    pf.set_model_provider(name="openai", provider=pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/chat/completions", api_key="new-key"))
    pf.list_model_providers()  # ["openai", "embed"]

OpenAI endpoints must be complete chat-completions or embeddings URLs, rather
than a base URL ending in ``/v1``. Model names can be included in the provider
configuration or supplied when calling an AI function.

A sole registered provider is selected automatically. Registering a second
provider requires an explicit default or provider selection; the first provider
does not remain the default merely because it was registered first. An explicitly
chosen default survives further registrations.

Registrations are process-global and survive clearing or replacing the
TableEnvironment. Convenience registration evaluates the provider identifier to
obtain its lookup name; named registration does not inspect the provider. Names
must be nonempty, non-whitespace strings. Invalid arguments or identifier errors
leave the previous registrations and default unchanged. Registration does not
create an environment, access Java, discover factories, or serialize a provider.
These APIs manage configuration; DataFrame prediction is a separate feature.

Use ``GenericProvider`` for raw Java option names and string values, or subclass
``ModelProvider`` for a reusable typed integration. The Java provider must already
be available in the application's classpath. Typed wrappers reject unknown
constructor arguments; use the generic form for options without a typed wrapper.

.. code-block:: python

    provider = pf.GenericProvider(
        "my-provider", **{"endpoint": "https://example.test/inference", "api-key": "key"})
    pf.set_model_provider("custom", provider)

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    ModelProvider
    OpenAIProvider
    TritonProvider
    GenericProvider
    set_model_provider
    set_default_model_provider
    list_model_providers
