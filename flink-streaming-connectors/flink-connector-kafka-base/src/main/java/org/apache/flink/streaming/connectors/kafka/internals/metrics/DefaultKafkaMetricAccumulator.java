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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;

public class DefaultKafkaMetricAccumulator implements Accumulator<Void, Double>, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaMetricAccumulator.class);

	protected boolean isMerged = false;
	protected double mergedValue;
	protected transient KafkaMetric kafkaMetric;


	public static DefaultKafkaMetricAccumulator createFor(Metric metric) {
		if(!(metric instanceof KafkaMetric)) {
			return null;
		}
		KafkaMetric kafkaMetric = (KafkaMetric) metric;
		Measurable measurable = getMeasurableFromKafkaMetric(kafkaMetric);
		if(measurable == null) {
			return null;
		}
		if (measurable instanceof Max) {
			return new MaxKafkaMetricAccumulator(kafkaMetric);
		} else if (measurable instanceof Min) {
			return new MinKafkaMetricAccumulator(kafkaMetric);
		} else if (measurable instanceof Avg) {
			return new AvgKafkaMetricAccumulator(kafkaMetric);
		} else {
			// fallback accumulator. works for Rate, Total, Count.
			return new DefaultKafkaMetricAccumulator(kafkaMetric);
		}
	}

	/**
	 * This utility method is using reflection to get the Measurable from the KafkaMetric.
	 * Since Kafka 0.9, Kafka is exposing the Measurable properly, but Kafka 0.8.2 does not yet expose it.
	 *
	 * @param kafkaMetric the metric to extract the field form
	 * @return Measurable type (or null in case of an error)
	 */
	protected static Measurable getMeasurableFromKafkaMetric(KafkaMetric kafkaMetric) {
		try {
			Field measurableField = kafkaMetric.getClass().getDeclaredField("measurable");
			measurableField.setAccessible(true);
			return (Measurable) measurableField.get(kafkaMetric);
		} catch (Throwable e) {
			LOG.warn("Unable to initialize Kafka metric: " + kafkaMetric, e);
			return null;
		}
	}


	DefaultKafkaMetricAccumulator(KafkaMetric kafkaMetric) {
		this.kafkaMetric = kafkaMetric;
	}

	@Override
	public void add(Void value) {
		// noop
	}

	@Override
	public Double getLocalValue() {
		if(isMerged && kafkaMetric == null) {
			return mergedValue;
		}
		return kafkaMetric.value();
	}

	@Override
	public void resetLocal() {
		// noop
	}

	@Override
	public void merge(Accumulator<Void, Double> other) {
		if(!(other instanceof DefaultKafkaMetricAccumulator)) {
			throw new RuntimeException("Trying to merge incompatible accumulators");
		}
		DefaultKafkaMetricAccumulator otherMetric = (DefaultKafkaMetricAccumulator) other;
		if(this.isMerged) {
			if(otherMetric.isMerged) {
				this.mergedValue += otherMetric.mergedValue;
			} else {
				this.mergedValue += otherMetric.getLocalValue();
			}
		} else {
			this.isMerged = true;
			if(otherMetric.isMerged) {
				this.mergedValue = this.getLocalValue() + otherMetric.mergedValue;
			} else {
				this.mergedValue = this.getLocalValue() + otherMetric.getLocalValue();
			}

		}
	}

	@Override
	public Accumulator<Void, Double> clone() {
		DefaultKafkaMetricAccumulator clone = new DefaultKafkaMetricAccumulator(this.kafkaMetric);
		clone.isMerged = this.isMerged;
		clone.mergedValue = this.mergedValue;
		return clone;
	}

	@Override
	public String toString() {
		if(isMerged) {
			return Double.toString(mergedValue);
		}
		if(kafkaMetric == null) {
			return "null";
		}
		return Double.toString(kafkaMetric.value());
	}

	// -------- custom serialization methods
	private void writeObject(ObjectOutputStream out) throws IOException {
		this.isMerged = true;
		this.mergedValue = kafkaMetric.value();
		out.defaultWriteObject();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
	}
}