# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
PyFlink AI inference module.

This module provides easy-to-use AI model inference capabilities for PyFlink,
including model lifecycle management, batch inference, and resource optimization.

Example:
    >>> from pyflink.datastream import StreamExecutionEnvironment
    >>> 
    >>> env = StreamExecutionEnvironment.get_execution_environment()
    >>> data = env.from_collection([{"text": "hello"}, {"text": "world"}])
    >>> 
    >>> result = data.infer(
    ...     model="sentence-transformers/all-MiniLM-L6-v2",
    ...     input_col="text",
    ...     output_col="embedding"
    ... )
"""

from pyflink.ml.inference.config import InferenceConfig
from pyflink.ml.inference.function import InferenceFunction, BatchInferenceFunction
from pyflink.ml.inference.lifecycle import ModelLifecycleManager
from pyflink.ml.inference.executor import BatchInferenceExecutor
from pyflink.ml.inference.metrics import InferenceMetrics

__all__ = [
    'InferenceConfig',
    'InferenceFunction',
    'BatchInferenceFunction',
    'ModelLifecycleManager',
    'BatchInferenceExecutor',
    'InferenceMetrics',
]
