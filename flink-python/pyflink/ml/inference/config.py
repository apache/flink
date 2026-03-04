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
Configuration for AI inference in PyFlink.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class InferenceConfig:
    """Configuration for AI model inference."""
    
    # Model configuration
    model: str
    """Model name or path (HuggingFace model or local path)"""
    
    input_col: str
    """Input column name"""
    
    output_col: str
    """Output column name"""
    
    task_type: str = "embedding"
    """Task type: embedding, classification, generation"""
    
    # Batch configuration
    batch_size: int = 32
    """Batch size for inference"""
    
    max_batch_timeout_ms: int = 100
    """Maximum timeout for batching in milliseconds"""
    
    # Model lifecycle
    warmup_enabled: bool = True
    """Whether to enable model warmup"""
    
    warmup_samples: int = 10
    """Number of warmup samples"""
    
    # Resource configuration
    device: str = "cpu"
    """Device for inference: cpu, cuda:0, cuda:1, etc."""
    
    num_workers: int = 1
    """Number of Python worker processes"""
    
    worker_memory: str = "2g"
    """Memory per worker"""
    
    # Model loading
    model_cache_dir: Optional[str] = None
    """Directory for caching downloaded models"""
    
    trust_remote_code: bool = False
    """Whether to trust remote code when loading models"""
    
    # Advanced options
    use_fp16: bool = False
    """Whether to use FP16 precision"""
    
    max_seq_length: Optional[int] = None
    """Maximum sequence length for tokenization"""
    
    def __post_init__(self):
        """Validate configuration."""
        if self.batch_size < 1:
            raise ValueError("batch_size must be positive")
        if self.max_batch_timeout_ms < 0:
            raise ValueError("max_batch_timeout_ms must be non-negative")
        if self.num_workers < 1:
            raise ValueError("num_workers must be positive")
        if self.task_type not in ["embedding", "classification", "generation"]:
            raise ValueError(f"Unsupported task_type: {self.task_type}")
