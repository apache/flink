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
Metrics for AI inference in PyFlink.
"""

import time
from typing import Any, Dict, List


class InferenceMetrics:
    """Tracks inference performance metrics."""
    
    def __init__(self):
        """Initialize metrics."""
        self.total_batches = 0
        self.total_records = 0
        self.total_errors = 0
        self.latencies: List[float] = []
        self.start_time = time.time()
        
    def record_batch(self, batch_size: int, latency_ms: float):
        """
        Record a successful batch inference.
        
        Args:
            batch_size: Number of records in the batch
            latency_ms: Inference latency in milliseconds
        """
        self.total_batches += 1
        self.total_records += batch_size
        self.latencies.append(latency_ms)
        
    def record_error(self):
        """Record an inference error."""
        self.total_errors += 1
        
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics.
        
        Returns:
            Dictionary containing metrics
        """
        elapsed_time = time.time() - self.start_time
        
        metrics = {
            'total_batches': self.total_batches,
            'total_records': self.total_records,
            'total_errors': self.total_errors,
            'elapsed_time_seconds': elapsed_time,
        }
        
        if self.latencies:
            metrics['avg_latency_ms'] = sum(self.latencies) / len(self.latencies)
            metrics['min_latency_ms'] = min(self.latencies)
            metrics['max_latency_ms'] = max(self.latencies)
            
            # Calculate percentiles
            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)
            metrics['p50_latency_ms'] = sorted_latencies[int(n * 0.5)]
            metrics['p95_latency_ms'] = sorted_latencies[int(n * 0.95)]
            metrics['p99_latency_ms'] = sorted_latencies[int(n * 0.99)]
            
        if elapsed_time > 0:
            metrics['throughput_records_per_second'] = self.total_records / elapsed_time
            
        if self.total_batches > 0:
            metrics['avg_batch_size'] = self.total_records / self.total_batches
            metrics['error_rate'] = self.total_errors / self.total_batches
            
        return metrics
        
    def reset(self):
        """Reset all metrics."""
        self.total_batches = 0
        self.total_records = 0
        self.total_errors = 0
        self.latencies.clear()
        self.start_time = time.time()
