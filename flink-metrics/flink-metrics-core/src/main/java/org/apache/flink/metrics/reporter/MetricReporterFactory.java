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

import java.util.Properties;

/**
 * {@link MetricReporter} factory.
 *
 * <p>Reporters that can be instantiated with a factory automatically qualify for being loaded as a plugin, so long as
 * the reporter jar is self-contained (excluding Flink dependencies) and contains a
 * {@code META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory} file containing the
 * qualified class name of the factory.
 *
 * <p>Reporters that previously relied on reflection for instantiation can use the {@link InstantiateViaFactory}
 * annotation to redirect reflection-base instantiation attempts to the factory instead.
 */
public interface MetricReporterFactory {

	/**
	 * Creates a new metric reporter.
	 *
	 * @param properties configured properties for the reporter
	 * @return created metric reporter
	 */
	MetricReporter createMetricReporter(final Properties properties);
}
