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

========
AI / LLM
========

Model Providers
---------------

Model providers configure connections and options for AI functions.
``OpenAIProvider`` and ``TritonProvider`` provide typed configurations for Flink's
built-in providers. See their API references below for parameters and defaults.
Use ``options`` to supply Java provider option names and string values directly.

Use ``set_model_provider`` to register a configuration. Pass a provider alone to
use its identifier as the name, or supply a name to distinguish configurations.
Use ``list_model_providers`` to inspect registered names:

.. code-block:: python

    import pyflink.dataframe as pf

    provider = pf.TritonProvider(endpoint="http://localhost:8000")
    pf.set_model_provider("inference", provider)
    pf.list_model_providers()  # ["inference"]

A single registered provider is selected automatically. When using several
providers, choose a default with ``set_default_model_provider("inference")``.
Registrations are shared within the Python process.

For other providers available in the application's classpath, use ``GenericProvider``
with provider-specific option names and string values, or subclass ``ModelProvider``
to create a reusable configuration.

Prediction
----------

Use ``df.llm.predict`` to infer a result for each row and append the output columns.
Supply one or more column names in model input order. The provider determines which
input types and number of columns it supports.

.. code-block:: python

    pf.set_model_provider("chat", pf.OpenAIProvider(
        endpoint="https://api.openai.com/v1/chat/completions", api_key="key", retry_num=3))
    questions = pf.from_records([{"question": "What is Apache Flink?"}])
    answers = questions.llm.predict(
        "question", provider="chat", model="gpt-6-luna")

The default output is one ``STRING`` column named ``output``. Set ``output_type``
to a SQL type string or a ``DataType`` to declare the provider's native output schema.

A ``DataType.struct(...)`` expands its top-level fields into named output columns.
Output field names must be unique and must not already occur in the input DataFrame.
The provider validates the output schema it supports.

.. code-block:: python

    # Name the prediction column "answer" instead of the default "output".
    answers = questions.llm.predict(
        "question", provider="chat", model="gpt-6-luna",
        output_type=pf.DataType.struct({"answer": pf.DataType.string()}))

You can pass in extra arguments to override the selected provider's constructor settings:

.. code-block:: python

    # Override the provider's retry_num parameter for this call.
    answers = questions.llm.predict(
        "question", provider="chat", model="gpt-6-luna", retry_num=0)

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

.. automethod:: pyflink.dataframe.llm.LLMAccessor.predict
