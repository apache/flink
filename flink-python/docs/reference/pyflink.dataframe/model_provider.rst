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

Register configurations under distinct lookup names. These names are independent
of the Java factory identifier, so several configurations of the same provider
can coexist:

.. code-block:: python

    import pyflink.dataframe as pf

    pf.set_model_provider("chat", pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/chat/completions", api_key="key"))
    pf.set_model_provider("embed", pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/embeddings", api_key="key"))
    pf.set_default_model_provider("chat")
    pf.list_model_providers()  # ["chat", "embed"]

OpenAI endpoints must be complete chat-completions or embeddings URLs, rather
than a base URL ending in ``/v1``. Model names can be included in the provider
configuration or supplied when a model is created.

A sole registered provider is selected automatically. Registering a second
provider requires an explicit default or provider selection; the first provider
does not remain the default merely because it was registered first. An explicitly
chosen default survives further registrations.

Registrations are process-global and survive clearing or replacing the
TableEnvironment. Duplicate lookup names are rejected. Registration does not
create an environment, access Java, discover factories, or serialize a custom
provider. These APIs manage configuration; DataFrame prediction is a separate
feature.

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
