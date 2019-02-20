/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.metrics.MetricSpec.Type.COUNTER;
import static org.apache.flink.metrics.MetricSpec.Type.GAUGE;
import static org.apache.flink.metrics.MetricSpec.Type.HISTOGRAM;
import static org.apache.flink.metrics.MetricSpec.Type.METER;

/**
 * A class that describes the specification of the metrics. The MetricSpec specifies the metric type and other
 * associated requirement for that type of the metric.
 */
public class MetricSpec {
	/** The metric type. */
	final Type type;
	/**
	 * The dependency metrics of this metric.
	 */
	final Set<String> dependencies;

	// ----------------------- Constructors ---------------------

	private MetricSpec(Type type) {
		this(type, Collections.emptySet());
	}

	/** We do not expect this MetricSpec to be constructed outside of this class. */
	private MetricSpec(Type type, Set<String> dependencies) {
		this.type = type;
		this.dependencies = dependencies;
	}

	/**
	 * Validate whether this metric spec could be defined in the given {@link MetricDef}.
	 * An exception should be thrown if the validation fails.
	 *
	 * @param metricDef the metric def to validate.
	 */
	public void validateMetricDef(MetricDef metricDef) throws Exception {

	}

	// -------------------  public methods to create metric specs -------------------------
	/**
	 * Get a default counter specification which uses {@link SimpleCounter}.
	 *
	 * @return a counter specification.
	 */
	public static CounterSpec counter() {
		return CounterSpec.INSTANCE;
	}

	/**
	 * Get a default meter specification which uses {@link MeterView}.
	 *
	 * @return a default meter specification.
	 */
	public static MeterSpec meter() {
		return MeterSpec.INSTANCE;
	}

	/**
	 * Create a meter specification which uses {@link MeterView}. Also register the underlying counter as
	 * a separate metric with the given name.
	 *
	 * @param counterMetricName the name of the separate metric for the underlying counter.
	 * @return a meter specification whose underlying counter is also registered as a separate metric with the
	 *         given name.
	 */
	public static MeterSpec meter(String counterMetricName) {
		return new MeterSpec(counterMetricName, MeterSpec.DEFAULT_TIME_SPAN_IN_SECONDS);
	}

	/**
	 * Create a meter specification which uses {@link MeterView} with the given time span setting.
	 *
	 * @param timeSpanInSeconds the time span for the underlying {@link MeterView}
	 * @return a meter specification with given time span setting.
	 */
	public static MeterSpec meter(int timeSpanInSeconds) {
		return new MeterSpec(null, timeSpanInSeconds);
	}

	/**
	 * Create a meter specification which uses {@link MeterView} with the given time span setting.
	 * Also register the underlying counter as a separate metric with the given name.
	 *
	 * @param counterMetricName the name of the separate metric for the underlying counter.
	 * @param timeSpanInSeconds the time span for the underlying {@link MeterView}
	 * @return
	 */
	public static MeterSpec meter(String counterMetricName, int timeSpanInSeconds) {
		return new MeterSpec(counterMetricName, timeSpanInSeconds);
	}

	/**
	 * Create a histogram spec which uses {@link com.codahale.metrics.Histogram} under the hood.
	 *
	 * @return a histogram spec.
	 */
	public static HistogramSpec histogram() {
		return HistogramSpec.INSTANCE;
	}

	/**
	 * Create a gauge spec.
	 *
	 * @return A gauge specification.
	 */
	public static GaugeSpec gauge() {
		return new GaugeSpec();
	}

	/**
	 * Create a metric spec for the given metric instance.
	 *
	 * @param metric the metric to create metric spec.
	 * @return the metric spec for the given metric instance.
	 */
	public static InstanceSpec of(Metric metric) {
		return new InstanceSpec(metric);
	}

	// ------------------  Metric specification classes for each metric type ---------------------

	/**
	 * Metric spec for counters.
	 */
	static class CounterSpec extends MetricSpec {
		/** A default instance to avoid unnecessary object creation. */
		private static final CounterSpec INSTANCE = new CounterSpec();

		private CounterSpec() {
			super(COUNTER);
		}
	}

	/**
	 * Metric spec for meters.
	 */
	static class MeterSpec extends MetricSpec {
		/** Default time span in seconds for the meters. */
		private static final int DEFAULT_TIME_SPAN_IN_SECONDS = 60;
		/** A default instance to avoid unnecessary object creation. */
		private static final MeterSpec INSTANCE = new MeterSpec(null, DEFAULT_TIME_SPAN_IN_SECONDS);

		/** The name for the separate metric of the underlying counter. */
		final String counterMetricName;
		/** The time span in seconds for the meter. */
		final int timeSpanInSeconds;

		/**
		 * There are two specification can be set for meters.
		 *
		 * @param counterMetricName when set, the counter used by the meter metric will also be registered as a
		 *                          separate metric with this given name. Otherwise, the counter will not have a
		 *                          separate metric.
		 * @param timeSpanInSeconds The time span used by the meter metric. See {@link MeterView} for details.
		 */
		private MeterSpec(String counterMetricName, int timeSpanInSeconds) {
			// Set the counter metric as a dependency.
			super(METER, getDependencies(counterMetricName));
			this.timeSpanInSeconds = timeSpanInSeconds;
			this.counterMetricName = counterMetricName;
		}

		@Override
		public void validateMetricDef(MetricDef metricDef) {
			Set<String> counterMetricDependants = metricDef.dependencies().get(counterMetricName);
			if (counterMetricDependants != null && !counterMetricDependants.isEmpty()) {
				throw new IllegalStateException(counterMetricName + " is already a dependant of metrics "
													+ counterMetricDependants);
			}
		}

		private static Set<String> getDependencies(String counterMetricName) {
			if (counterMetricName == null || counterMetricName.isEmpty()) {
				return Collections.emptySet();
			} else {
				return Collections.singleton(counterMetricName);
			}
		}
	}

	/**
	 * Metric spec for histograms.
	 */
	static class HistogramSpec extends MetricSpec {
		/** A default instance to avoid unnecessary object creation. */
		private static final HistogramSpec INSTANCE = new HistogramSpec();

		HistogramSpec() {
			super(HISTOGRAM);
		}
	}

	/**
	 * Metric spec for gauge.
	 */
	static class GaugeSpec extends MetricSpec {
		private GaugeSpec() {
			super(GAUGE);
		}
	}

	/**
	 * Metric spec for given instance. Users may use this
	 */
	static class InstanceSpec extends MetricSpec {
		final Metric metric;
		InstanceSpec(Metric metric) {
			super(Type.of(metric));
			this.metric = metric;
		}
	}

	/**
	 * An enum for quick metric type look up.
	 */
	enum Type {
		COUNTER(Counter.class),
		METER(Meter.class),
		HISTOGRAM(Histogram.class),
		GAUGE(Gauge.class);

		final Class<? extends Metric> metricClass;

		Type(Class<? extends Metric> metricClass) {
			this.metricClass = metricClass;
		}

		public static Type of(Metric metric) {
			for (Type type : Type.values()) {
				if (type.metricClass.isAssignableFrom(metric.getClass())) {
					return type;
				}
			}
			throw new IllegalArgumentException("Unsupported metric type " + metric.getClass());
		}
	}
}
