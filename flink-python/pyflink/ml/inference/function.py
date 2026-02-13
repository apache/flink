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
Inference function for PyFlink DataStream API.
"""

import logging
from typing import Any, Dict, Iterable

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.ml.inference.config import InferenceConfig
from pyflink.ml.inference.executor import BatchInferenceExecutor
from pyflink.ml.inference.lifecycle import ModelLifecycleManager


logger = logging.getLogger(__name__)


class InferenceFunction(MapFunction):
    """
    MapFunction that performs AI model inference on DataStream elements.
    
    This function manages the model lifecycle including loading, warmup,
    inference, and cleanup.
    """
    
    def __init__(self, config: InferenceConfig):
        """
        Initialize the inference function.
        
        Args:
            config: Inference configuration
        """
        self.config = config
        self.model_manager = None
        self.executor = None
        self.batch_buffer = []
        self.last_flush_time = 0
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize model and resources."""
        logger.info("Initializing InferenceFunction")
        
        try:
            # Create model lifecycle manager
            self.model_manager = ModelLifecycleManager(self.config)
            
            # Load model
            self.model_manager.load_model()
            
            # Warmup model
            if self.config.warmup_enabled:
                self.model_manager.warmup()
                
            # Create executor
            self.executor = BatchInferenceExecutor(self.model_manager, self.config)
            
            # Initialize batch buffer
            self.batch_buffer = []
            self.last_flush_time = self._current_time_ms()
            
            logger.info("InferenceFunction initialized successfully")
            logger.info(f"Model info: {self.model_manager.get_model_info()}")
            
        except Exception as e:
            logger.error(f"Failed to initialize InferenceFunction: {e}")
            raise
            
    def map(self, value: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single element.
        
        For now, we process elements one by one. In future versions,
        this will be integrated with AsyncBatchFunction for true batching.
        
        Args:
            value: Input record
            
        Returns:
            Output record with inference result
        """
        # For MVP, process single records
        # TODO: Integrate with AsyncBatchFunction for batching
        results = self.executor.infer_batch([value])
        return results[0] if results else value
        
    def close(self):
        """Cleanup resources."""
        logger.info("Closing InferenceFunction")
        
        try:
            # Flush remaining batch
            if self.batch_buffer:
                self._flush_batch()
                
            # Unload model
            if self.model_manager:
                logger.info(f"Final metrics: {self.executor.get_metrics()}")
                self.model_manager.unload_model()
                
            logger.info("InferenceFunction closed successfully")
            
        except Exception as e:
            logger.error(f"Error during InferenceFunction cleanup: {e}")
            
    def _flush_batch(self):
        """Flush the current batch (for future batching implementation)."""
        if not self.batch_buffer:
            return
            
        try:
            results = self.executor.infer_batch(self.batch_buffer)
            # TODO: Emit results when integrated with AsyncBatchFunction
            self.batch_buffer.clear()
            self.last_flush_time = self._current_time_ms()
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
            self.batch_buffer.clear()
            
    @staticmethod
    def _current_time_ms() -> int:
        """Get current time in milliseconds."""
        import time
        return int(time.time() * 1000)


class BatchInferenceFunction(MapFunction):
    """
    Batch inference function that processes multiple records at once.
    
    This is a simpler version that expects pre-batched inputs.
    """
    
    def __init__(self, config: InferenceConfig):
        """Initialize with config."""
        self.config = config
        self.model_manager = None
        self.executor = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize resources."""
        self.model_manager = ModelLifecycleManager(self.config)
        self.model_manager.load_model()
        self.model_manager.warmup()
        self.executor = BatchInferenceExecutor(self.model_manager, self.config)
        
    def map(self, batch: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Process a batch of records."""
        batch_list = list(batch)
        return self.executor.infer_batch(batch_list)
        
    def close(self):
        """Cleanup resources."""
        if self.model_manager:
            self.model_manager.unload_model()
