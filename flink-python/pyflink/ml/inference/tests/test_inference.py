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

"""
Unit tests for PyFlink ML inference module.
"""

import unittest
from unittest.mock import MagicMock, patch

from pyflink.ml.inference import (
    InferenceConfig,
    InferenceFunction,
    ModelLifecycleManager,
    BatchInferenceExecutor,
    InferenceMetrics
)


class TestInferenceConfig(unittest.TestCase):
    """Tests for InferenceConfig."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = InferenceConfig(
            model="test-model",
            input_col="input",
            output_col="output"
        )
        
        self.assertEqual(config.model, "test-model")
        self.assertEqual(config.input_col, "input")
        self.assertEqual(config.output_col, "output")
        self.assertEqual(config.batch_size, 32)
        self.assertEqual(config.device, "cpu")
        self.assertEqual(config.task_type, "embedding")
        
    def test_invalid_batch_size(self):
        """Test validation of batch size."""
        with self.assertRaises(ValueError):
            InferenceConfig(
                model="test-model",
                input_col="input",
                output_col="output",
                batch_size=0
            )
            
    def test_invalid_task_type(self):
        """Test validation of task type."""
        with self.assertRaises(ValueError):
            InferenceConfig(
                model="test-model",
                input_col="input",
                output_col="output",
                task_type="invalid"
            )


class TestInferenceMetrics(unittest.TestCase):
    """Tests for InferenceMetrics."""
    
    def test_record_batch(self):
        """Test recording batch metrics."""
        metrics = InferenceMetrics()
        
        metrics.record_batch(batch_size=10, latency_ms=50.0)
        metrics.record_batch(batch_size=20, latency_ms=100.0)
        
        result = metrics.get_metrics()
        
        self.assertEqual(result['total_batches'], 2)
        self.assertEqual(result['total_records'], 30)
        self.assertEqual(result['avg_latency_ms'], 75.0)
        self.assertEqual(result['min_latency_ms'], 50.0)
        self.assertEqual(result['max_latency_ms'], 100.0)
        
    def test_record_error(self):
        """Test recording errors."""
        metrics = InferenceMetrics()
        
        metrics.record_batch(batch_size=10, latency_ms=50.0)
        metrics.record_error()
        metrics.record_error()
        
        result = metrics.get_metrics()
        
        self.assertEqual(result['total_errors'], 2)
        self.assertEqual(result['error_rate'], 2.0)  # 2 errors / 1 batch
        
    def test_percentiles(self):
        """Test latency percentile calculations."""
        metrics = InferenceMetrics()
        
        # Record 100 batches with varying latencies
        for i in range(100):
            metrics.record_batch(batch_size=10, latency_ms=float(i))
            
        result = metrics.get_metrics()
        
        self.assertAlmostEqual(result['p50_latency_ms'], 50.0, delta=1.0)
        self.assertAlmostEqual(result['p95_latency_ms'], 95.0, delta=1.0)
        self.assertAlmostEqual(result['p99_latency_ms'], 99.0, delta=1.0)


@patch('pyflink.ml.inference.lifecycle.torch')
@patch('pyflink.ml.inference.lifecycle.AutoModel')
@patch('pyflink.ml.inference.lifecycle.AutoTokenizer')
class TestModelLifecycleManager(unittest.TestCase):
    """Tests for ModelLifecycleManager."""
    
    def test_load_model(self, mock_tokenizer, mock_model, mock_torch):
        """Test model loading."""
        config = InferenceConfig(
            model="test-model",
            input_col="input",
            output_col="output",
            device="cpu"
        )
        
        # Setup mocks
        mock_torch.device.return_value = MagicMock()
        mock_torch.cuda.is_available.return_value = False
        
        mock_model_instance = MagicMock()
        mock_model.from_pretrained.return_value = mock_model_instance
        
        mock_tokenizer_instance = MagicMock()
        mock_tokenizer.from_pretrained.return_value = mock_tokenizer_instance
        
        # Load model
        manager = ModelLifecycleManager(config)
        manager.load_model()
        
        # Verify
        self.assertTrue(manager.is_loaded())
        mock_model.from_pretrained.assert_called_once()
        mock_tokenizer.from_pretrained.assert_called_once()
        mock_model_instance.eval.assert_called_once()
        
    def test_warmup(self, mock_tokenizer, mock_model, mock_torch):
        """Test model warmup."""
        config = InferenceConfig(
            model="test-model",
            input_col="input",
            output_col="output",
            warmup_enabled=True,
            warmup_samples=5
        )
        
        # Setup mocks
        mock_torch.device.return_value = MagicMock(type="cpu")
        mock_torch.cuda.is_available.return_value = False
        mock_torch.no_grad.return_value.__enter__ = MagicMock()
        mock_torch.no_grad.return_value.__exit__ = MagicMock()
        
        mock_model_instance = MagicMock()
        mock_model.from_pretrained.return_value = mock_model_instance
        
        mock_tokenizer_instance = MagicMock()
        mock_tokenizer_instance.return_value = {}
        mock_tokenizer.from_pretrained.return_value = mock_tokenizer_instance
        
        # Load and warmup
        manager = ModelLifecycleManager(config)
        manager.load_model()
        manager.warmup()
        
        # Verify model was called multiple times for warmup
        self.assertGreater(mock_model_instance.call_count, 0)


@patch('pyflink.ml.inference.executor.torch')
class TestBatchInferenceExecutor(unittest.TestCase):
    """Tests for BatchInferenceExecutor."""
    
    def test_infer_batch_embedding(self, mock_torch):
        """Test batch inference for embeddings."""
        config = InferenceConfig(
            model="test-model",
            input_col="text",
            output_col="embedding",
            task_type="embedding"
        )
        
        # Setup mocks
        mock_manager = MagicMock()
        mock_manager.tokenizer.return_value = {}
        mock_manager.device = MagicMock()
        
        mock_outputs = MagicMock()
        mock_outputs.last_hidden_state = MagicMock()
        mock_outputs.last_hidden_state.__getitem__ = MagicMock(
            return_value=MagicMock(
                cpu=MagicMock(
                    return_value=MagicMock(
                        numpy=MagicMock(return_value=[[0.1, 0.2, 0.3]])
                    )
                )
            )
        )
        
        mock_manager.model.return_value = mock_outputs
        
        executor = BatchInferenceExecutor(mock_manager, config)
        
        # Execute inference
        batch = [{"text": "test"}]
        results = executor.infer_batch(batch)
        
        # Verify
        self.assertEqual(len(results), 1)
        self.assertIn("embedding", results[0])
        
    def test_infer_batch_empty(self, mock_torch):
        """Test inference with empty batch."""
        config = InferenceConfig(
            model="test-model",
            input_col="text",
            output_col="output"
        )
        
        executor = BatchInferenceExecutor(MagicMock(), config)
        results = executor.infer_batch([])
        
        self.assertEqual(len(results), 0)


class TestInferenceFunction(unittest.TestCase):
    """Tests for InferenceFunction."""
    
    @patch('pyflink.ml.inference.function.ModelLifecycleManager')
    @patch('pyflink.ml.inference.function.BatchInferenceExecutor')
    def test_map(self, mock_executor_class, mock_manager_class):
        """Test map function."""
        config = InferenceConfig(
            model="test-model",
            input_col="text",
            output_col="output"
        )
        
        # Setup mocks
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_executor = MagicMock()
        mock_executor.infer_batch.return_value = [{"text": "test", "output": "result"}]
        mock_executor_class.return_value = mock_executor
        
        # Create function and open
        func = InferenceFunction(config)
        func.open(MagicMock())
        
        # Execute map
        result = func.map({"text": "test"})
        
        # Verify
        self.assertEqual(result["output"], "result")
        mock_executor.infer_batch.assert_called_once()


if __name__ == '__main__':
    unittest.main()
