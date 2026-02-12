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
Batch inference executor for PyFlink AI inference.
"""

import logging
import time
from typing import Any, Dict, List

import numpy as np

from pyflink.ml.inference.config import InferenceConfig
from pyflink.ml.inference.lifecycle import ModelLifecycleManager
from pyflink.ml.inference.metrics import InferenceMetrics


logger = logging.getLogger(__name__)


class BatchInferenceExecutor:
    """Executes batch inference with model lifecycle management."""
    
    def __init__(self, model_manager: ModelLifecycleManager, config: InferenceConfig):
        """
        Initialize the batch inference executor.
        
        Args:
            model_manager: Model lifecycle manager
            config: Inference configuration
        """
        self.model_manager = model_manager
        self.config = config
        self.metrics = InferenceMetrics()
        
    def infer_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute batch inference.
        
        Args:
            batch: List of input records
            
        Returns:
            List of output records with inference results
        """
        if not batch:
            return []
            
        start_time = time.time()
        batch_size = len(batch)
        
        try:
            # Extract input column
            input_texts = [item.get(self.config.input_col) for item in batch]
            
            # Validate inputs
            if any(text is None for text in input_texts):
                raise ValueError(f"Missing input column: {self.config.input_col}")
                
            # Execute inference based on task type
            if self.config.task_type == "embedding":
                results = self._infer_embedding(input_texts, batch)
            elif self.config.task_type == "classification":
                results = self._infer_classification(input_texts, batch)
            elif self.config.task_type == "generation":
                results = self._infer_generation(input_texts, batch)
            else:
                raise ValueError(f"Unsupported task type: {self.config.task_type}")
                
            # Record metrics
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_batch(batch_size, latency_ms)
            
            logger.debug(f"Batch inference completed: size={batch_size}, "
                        f"latency={latency_ms:.2f}ms")
            
            return results
            
        except Exception as e:
            self.metrics.record_error()
            logger.error(f"Batch inference failed: {e}")
            raise
            
    def _infer_embedding(self, input_texts: List[str], 
                        original_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute embedding inference."""
        import torch
        
        # Tokenize
        inputs = self.model_manager.tokenizer(
            input_texts,
            padding=True,
            truncation=True,
            max_length=self.config.max_seq_length,
            return_tensors="pt"
        )
        
        # Move to device
        inputs = {k: v.to(self.model_manager.device) for k, v in inputs.items()}
        
        # Inference
        with torch.no_grad():
            outputs = self.model_manager.model(**inputs)
            
        # Extract embeddings (use [CLS] token representation)
        embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
        
        # Combine with original data
        results = []
        for item, emb in zip(original_batch, embeddings):
            result = item.copy()
            result[self.config.output_col] = emb.tolist()
            results.append(result)
            
        return results
        
    def _infer_classification(self, input_texts: List[str],
                             original_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute classification inference."""
        import torch
        from transformers import AutoModelForSequenceClassification
        
        # For classification, we need a classification-specific model
        if not hasattr(self.model_manager.model, 'classifier'):
            logger.warning("Model doesn't have classifier head, using logits from last layer")
            
        # Tokenize
        inputs = self.model_manager.tokenizer(
            input_texts,
            padding=True,
            truncation=True,
            max_length=self.config.max_seq_length,
            return_tensors="pt"
        )
        
        # Move to device
        inputs = {k: v.to(self.model_manager.device) for k, v in inputs.items()}
        
        # Inference
        with torch.no_grad():
            outputs = self.model_manager.model(**inputs)
            
        # Get predictions
        if hasattr(outputs, 'logits'):
            logits = outputs.logits.cpu().numpy()
        else:
            # Fallback to pooling if no logits
            logits = outputs.last_hidden_state[:, 0, :].cpu().numpy()
            
        predictions = np.argmax(logits, axis=-1)
        probabilities = self._softmax(logits)
        
        # Combine with original data
        results = []
        for item, pred, prob in zip(original_batch, predictions, probabilities):
            result = item.copy()
            result[self.config.output_col] = {
                'label': int(pred),
                'probabilities': prob.tolist()
            }
            results.append(result)
            
        return results
        
    def _infer_generation(self, input_texts: List[str],
                         original_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute text generation inference."""
        import torch
        
        # Tokenize
        inputs = self.model_manager.tokenizer(
            input_texts,
            padding=True,
            truncation=True,
            max_length=self.config.max_seq_length,
            return_tensors="pt"
        )
        
        # Move to device
        inputs = {k: v.to(self.model_manager.device) for k, v in inputs.items()}
        
        # Generate
        with torch.no_grad():
            output_ids = self.model_manager.model.generate(
                **inputs,
                max_length=self.config.max_seq_length or 100,
                num_return_sequences=1
            )
            
        # Decode
        generated_texts = self.model_manager.tokenizer.batch_decode(
            output_ids,
            skip_special_tokens=True
        )
        
        # Combine with original data
        results = []
        for item, text in zip(original_batch, generated_texts):
            result = item.copy()
            result[self.config.output_col] = text
            results.append(result)
            
        return results
        
    @staticmethod
    def _softmax(x: np.ndarray) -> np.ndarray:
        """Apply softmax to convert logits to probabilities."""
        exp_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
        return exp_x / np.sum(exp_x, axis=-1, keepdims=True)
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get inference metrics."""
        return self.metrics.get_metrics()
