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
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

public class AvgKafkaMetricAccumulator extends DefaultKafkaMetricAccumulator {
	private static final Logger LOG = LoggerFactory.getLogger(AvgKafkaMetricAccumulator.class);

	/** The last sum/count before the serialization  **/
	private AvgSumCount lastSumCount;

	public AvgKafkaMetricAccumulator(KafkaMetric kafkaMetric) {
		super(kafkaMetric);
	}

	@Override
	public void merge(Accumulator<Void, Double> other) {
		if(!(other instanceof AvgKafkaMetricAccumulator)) {
			throw new RuntimeException("Trying to merge incompatible accumulators: "+this+" with "+other);
		}
		AvgKafkaMetricAccumulator otherMetric = (AvgKafkaMetricAccumulator) other;

		AvgSumCount thisAvg;
		if(this.lastSumCount == null) {
			Measurable thisMeasurable = DefaultKafkaMetricAccumulator.getMeasurableFromKafkaMetric(this.kafkaMetric);
			if (!(thisMeasurable instanceof Avg)) {
				throw new RuntimeException("Must be of type Avg");
			}
			thisAvg = getAvgSumCount((Avg) thisMeasurable);
		} else {
			thisAvg = this.lastSumCount;
		}

		AvgSumCount otherAvg;
		if(otherMetric.lastSumCount == null) {
			Measurable otherMeasurable = DefaultKafkaMetricAccumulator.getMeasurableFromKafkaMetric(otherMetric.kafkaMetric);
			if(!(otherMeasurable instanceof Avg) ) {
				throw new RuntimeException("Must be of type Avg");
			}
			otherAvg = getAvgSumCount((Avg) otherMeasurable);
		} else {
			otherAvg = otherMetric.lastSumCount;
		}

		thisAvg.count += otherAvg.count;
		thisAvg.sum += otherAvg.sum;
		this.mergedValue = thisAvg.sum / thisAvg.count;
	}

	@Override
	public Accumulator<Void, Double> clone() {
		AvgKafkaMetricAccumulator clone = new AvgKafkaMetricAccumulator(kafkaMetric);
		clone.lastSumCount = this.lastSumCount;
		clone.isMerged = this.isMerged;
		clone.mergedValue = this.mergedValue;
		return clone;
	}

	// ------------ Utilities

	private static class AvgSumCount implements Serializable {
		double sum;
		long count;

		@Override
		public String toString() {
			return "AvgSumCount{" +
					"sum=" + sum +
					", count=" + count +
					", avg="+(sum/count)+"}";
		}
	}

	/**
	 * Extracts sum and count from Avg using reflection
	 *
	 * @param avg Avg SampledStat from Kafka
	 * @return A KV pair with the average's sum and count
	 */
	private static AvgSumCount getAvgSumCount(Avg avg) {
		try {
			Field samplesField = SampledStat.class.getDeclaredField("samples");
			Field sampleValue = Class.forName("org.apache.kafka.common.metrics.stats.SampledStat$Sample").getDeclaredField("value");
			Field sampleEventCount = Class.forName("org.apache.kafka.common.metrics.stats.SampledStat$Sample").getDeclaredField("eventCount");
			samplesField.setAccessible(true);
			sampleValue.setAccessible(true);
			sampleEventCount.setAccessible(true);
			List samples = (List) samplesField.get(avg);
			AvgSumCount res = new AvgSumCount();
			for(int i = 0; i < samples.size(); i++) {
				res.sum += (double)sampleValue.get(samples.get(i));
				res.count += (long)sampleEventCount.get(samples.get(i));
			}
			return res;
		} catch(Throwable t) {
			throw new RuntimeException("Unable to extract sum and count from Avg using reflection. " +
					"You can turn off the metrics from Flink's Kafka connector if this issue persists.", t);
		}
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		Measurable thisMeasurable = DefaultKafkaMetricAccumulator.getMeasurableFromKafkaMetric(this.kafkaMetric);
		if(!(thisMeasurable instanceof Avg) ) {
			throw new RuntimeException("Must be of type Avg");
		}
		this.lastSumCount = getAvgSumCount((Avg) thisMeasurable);
		out.defaultWriteObject();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
	}
}
