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
Model lifecycle management for PyFlink AI inference.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from pyflink.ml.inference.config import InferenceConfig


logger = logging.getLogger(__name__)


class ModelLifecycleManager:
    """Manages the lifecycle of ML models including loading, warmup, and unloading."""
    
    def __init__(self, config: InferenceConfig):
        """
        Initialize the model lifecycle manager.
        
        Args:
            config: Inference configuration
        """
        self.config = config
        self.model = None
        self.tokenizer = None
        self.device = None
        self._model_loaded = False
        
    def load_model(self):
        """Load the model and tokenizer."""
        if self._model_loaded:
            logger.info("Model already loaded, skipping")
            return
            
        logger.info(f"Loading model: {self.config.model}")
        start_time = time.time()
        
        try:
            # Import here to avoid requiring torch/transformers at import time
            import torch
            from transformers import AutoModel, AutoTokenizer
            
            # Setup device
            self.device = self._setup_device()
            logger.info(f"Using device: {self.device}")
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.config.model,
                cache_dir=self.config.model_cache_dir,
                trust_remote_code=self.config.trust_remote_code
            )
            
            # Load model
            self.model = AutoModel.from_pretrained(
                self.config.model,
                cache_dir=self.config.model_cache_dir,
                trust_remote_code=self.config.trust_remote_code
            )
            
            # Move to device
            self.model.to(self.device)
            self.model.eval()
            
            # Use FP16 if requested
            if self.config.use_fp16 and self.device.type == "cuda":
                self.model.half()
                
            self._model_loaded = True
            
            load_time = time.time() - start_time
            logger.info(f"Model loaded successfully in {load_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
            
    def warmup(self):
        """Perform model warmup with dummy inputs."""
        if not self.config.warmup_enabled:
            logger.info("Model warmup disabled, skipping")
            return
            
        if not self._model_loaded:
            raise RuntimeError("Model must be loaded before warmup")
            
        logger.info(f"Starting model warmup with {self.config.warmup_samples} samples")
        start_time = time.time()
        
        try:
            import torch
            
            # Generate dummy inputs
            dummy_texts = ["warmup sample"] * self.config.warmup_samples
            
            with torch.no_grad():
                for _ in range(3):  # 3 warmup rounds
                    inputs = self.tokenizer(
                        dummy_texts,
                        padding=True,
                        truncation=True,
                        max_length=self.config.max_seq_length,
                        return_tensors="pt"
                    )
                    
                    inputs = {k: v.to(self.device) for k, v in inputs.items()}
                    _ = self.model(**inputs)
                    
            # CUDA synchronization and cache cleanup
            if self.device.type == "cuda":
                torch.cuda.synchronize()
                torch.cuda.empty_cache()
                
            warmup_time = time.time() - start_time
            logger.info(f"Model warmup completed in {warmup_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Model warmup failed: {e}")
            raise
            
    def unload_model(self):
        """Unload the model and free resources."""
        if not self._model_loaded:
            return
            
        logger.info("Unloading model")
        
        try:
            import torch
            
            del self.model
            del self.tokenizer
            
            if self.device and self.device.type == "cuda":
                torch.cuda.empty_cache()
                
            self._model_loaded = False
            logger.info("Model unloaded successfully")
            
        except Exception as e:
            logger.error(f"Error during model unloading: {e}")
            
    def _setup_device(self):
        """Setup the computation device."""
        import torch
        
        device_str = self.config.device
        
        if device_str.startswith("cuda"):
            if not torch.cuda.is_available():
                logger.warning("CUDA not available, falling back to CPU")
                return torch.device("cpu")
            return torch.device(device_str)
        else:
            return torch.device("cpu")
            
    def is_loaded(self) -> bool:
        """Check if model is loaded."""
        return self._model_loaded
        
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information."""
        return {
            "model_name": self.config.model,
            "device": str(self.device) if self.device else "unknown",
            "loaded": self._model_loaded,
            "task_type": self.config.task_type
        }
