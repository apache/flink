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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Utilities to fallback to new options from the legacy ones for the backwards compatibility.
 *
 * <p>If {@link LegacyMemoryOptions} are set, they are interpreted as other new memory options for the backwards
 * compatibility.
 */
public class MemoryBackwardsCompatibilityUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MemoryBackwardsCompatibilityUtils.class);

	private final LegacyMemoryOptions legacyMemoryOptions;

	public MemoryBackwardsCompatibilityUtils(LegacyMemoryOptions legacyMemoryOptions) {
		this.legacyMemoryOptions = legacyMemoryOptions;
	}

	public Configuration getConfWithLegacyHeapSizeMappedToNewConfigOption(
		Configuration configuration,
		ConfigOption<MemorySize> configOption) {
		if (configuration.contains(configOption)) {
			return configuration;
		}
		return getLegacyHeapMemoryIfExplicitlyConfigured(configuration)
			.map(legacyHeapSize -> {
				Configuration copiedConfig = new Configuration(configuration);
				copiedConfig.set(configOption, legacyHeapSize);
				LOG.info(
					"'{}' is not specified, use the configured deprecated task manager heap value ({}) for it.",
					configOption.key(),
					legacyHeapSize.toHumanReadableString());
				return copiedConfig;
			}).orElse(configuration);
	}

	private Optional<MemorySize> getLegacyHeapMemoryIfExplicitlyConfigured(Configuration configuration) {
		@SuppressWarnings("CallToSystemGetenv")
		String totalProcessEnv = System.getenv(legacyMemoryOptions.getEnvVar());
		if (totalProcessEnv != null) {
			//noinspection OverlyBroadCatchBlock
			try {
				return Optional.of(MemorySize.parse(totalProcessEnv));
			} catch (Throwable t) {
				throw new IllegalConfigurationException(
					"Cannot read total process memory size from environment variable value " + totalProcessEnv + '.',
					t);
			}
		}

		if (configuration.contains(legacyMemoryOptions.getHeap())) {
			return Optional.of(ProcessMemoryUtils.getMemorySizeFromConfig(configuration, legacyMemoryOptions.getHeap()));
		}

		if (configuration.contains(legacyMemoryOptions.getHeapMb())) {
			final long legacyHeapMemoryMB = configuration.getInteger(legacyMemoryOptions.getHeapMb());
			if (legacyHeapMemoryMB < 0) {
				throw new IllegalConfigurationException(
					"Configured total process memory size (" + legacyHeapMemoryMB + "MB) must not be less than 0.");
			}
			//noinspection MagicNumber
			return Optional.of(new MemorySize(legacyHeapMemoryMB << 20)); // megabytes to bytes;
		}

		return Optional.empty();
	}
}
