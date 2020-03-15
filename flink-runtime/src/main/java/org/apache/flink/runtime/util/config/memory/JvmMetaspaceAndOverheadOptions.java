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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

/**
 * Options to calculate JVM Metaspace and Overhead.
 */
public class JvmMetaspaceAndOverheadOptions {
	private final ConfigOption<MemorySize> jvmMetaspaceOption;
	private final ConfigOption<MemorySize> jvmOverheadMin;
	private final ConfigOption<MemorySize> jvmOverheadMax;
	private final ConfigOption<Float> jvmOverheadFraction;

	public JvmMetaspaceAndOverheadOptions(
			ConfigOption<MemorySize> jvmMetaspaceOption,
			ConfigOption<MemorySize> jvmOverheadMin,
			ConfigOption<MemorySize> jvmOverheadMax,
			ConfigOption<Float> jvmOverheadFraction) {
		this.jvmMetaspaceOption = jvmMetaspaceOption;
		this.jvmOverheadMin = jvmOverheadMin;
		this.jvmOverheadMax = jvmOverheadMax;
		this.jvmOverheadFraction = jvmOverheadFraction;
	}

	ConfigOption<MemorySize> getJvmMetaspaceOption() {
		return jvmMetaspaceOption;
	}

	ConfigOption<MemorySize> getJvmOverheadMin() {
		return jvmOverheadMin;
	}

	ConfigOption<MemorySize> getJvmOverheadMax() {
		return jvmOverheadMax;
	}

	ConfigOption<Float> getJvmOverheadFraction() {
		return jvmOverheadFraction;
	}
}
