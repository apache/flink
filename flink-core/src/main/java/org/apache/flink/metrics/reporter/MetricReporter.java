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
package org.apache.flink.metrics.reporter;

import com.codahale.metrics.Reporter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;

import java.util.List;

/**
 * Reporters are used to export {@link org.apache.flink.metrics.Metric}s to an external backend.
 * <p>
 * Reporters are instantiated generically and must have a no-argument constructor.
 */
@PublicEvolving
public interface MetricReporter extends Reporter {
	
	/**
	 * Configures this reporter. Since reporters are instantiated generically and hence parameter-less,
	 * this method is the place where the reporters set their basic fields based on configuration values.
	 * <p>
	 * This method is always called first on a newly instantiated reporter.
	 *
	 * @param config The configuration with all parameters.
	 */
	void open(Configuration config);

	/**
	 * Closes this reporter. Should be used to close channels, streams and release resources.
	 */
	void close();

	/**
	 * Called when a new {@link org.apache.flink.metrics.Metric} was added.
	 *
	 * @param metric metric that was added
	 * @param name   name of the metric
	 */
	void notifyOfAddedMetric(Metric metric, String name);

	/**
	 * Called when a {@link org.apache.flink.metrics.Metric} was removed.
	 *
	 * @param metric metric that was removed
	 * @param name   name of the metric
	 */
	void notifyOfRemovedMetric(Metric metric, String name);

	/**
	 * Generates the reported name of a metric based on it's hierarchy/scope and associated name.
	 *
	 * @param name  name of the metric
	 * @param scope hierarchy/scope of the metric
	 * @return reported name
	 */
	String generateName(String name, List<String> scope);
}
