/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.kafka.common.metrics.KafkaMetric;

public class MinKafkaMetricAccumulator extends DefaultKafkaMetricAccumulator {

	public MinKafkaMetricAccumulator(KafkaMetric kafkaMetric) {
		super(kafkaMetric);
	}

	@Override
	public void merge(Accumulator<Void, Double> other) {
		if(!(other instanceof MinKafkaMetricAccumulator)) {
			throw new RuntimeException("Trying to merge incompatible accumulators");
		}
		MinKafkaMetricAccumulator otherMetric = (MinKafkaMetricAccumulator) other;
		if(this.isMerged) {
			if(otherMetric.isMerged) {
				this.mergedValue = Math.min(this.mergedValue, otherMetric.mergedValue);
			} else {
				this.mergedValue = Math.min(this.mergedValue, otherMetric.getLocalValue());
			}
		} else {
			this.isMerged = true;
			if(otherMetric.isMerged) {
				this.mergedValue = Math.min(this.getLocalValue(), otherMetric.mergedValue);
			} else {
				this.mergedValue = Math.min(this.getLocalValue(), otherMetric.getLocalValue());
			}
		}
	}

	@Override
	public Accumulator<Void, Double> clone() {
		MinKafkaMetricAccumulator clone = new MinKafkaMetricAccumulator(this.kafkaMetric);
		clone.isMerged = this.isMerged;
		clone.mergedValue = this.mergedValue;
		return clone;
	}
}
