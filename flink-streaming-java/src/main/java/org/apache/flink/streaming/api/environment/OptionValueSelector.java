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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

/**
 * Selects a value from a list of alternatives for unset {@link ConfigOption}s.
 */
@PublicEvolving
public interface OptionValueSelector extends Serializable {
	<T> T select(ConfigOption<T> option, T... alternatives);

	static OptionValueSelector create(boolean random) {
		if (!random) {
			// always pick default option
			return DefaultOptionValueSelector.INSTANCE;
		}

		// lazy initialization only when a value is really selected
		return new OptionValueSelector() {
			private OptionValueSelector randomSelector;

			@Override
			public <T> T select(ConfigOption<T> option, T... alternatives) {
				if (randomSelector == null) {
					randomSelector = PseudoRandomValueSelector.create();
				}
				return randomSelector.select(option, alternatives);
			}
		};
	}
}

@Internal
class DefaultOptionValueSelector implements OptionValueSelector {
	static final DefaultOptionValueSelector INSTANCE = new DefaultOptionValueSelector();

	@Override
	public <T> T select(ConfigOption<T> option, T... alternatives) {
		return option.defaultValue();
	}
}

@Internal
class PseudoRandomValueSelector implements OptionValueSelector {
	private Function<Integer, Integer> randomValueSupplier;

	private static final Logger LOG = LoggerFactory.getLogger(PseudoRandomValueSelector.class);

	private <T extends Function<Integer, Integer> & Serializable> PseudoRandomValueSelector(T randomValueSupplier) {
		this.randomValueSupplier = randomValueSupplier;
	}

	@Override
	public <T> T select(ConfigOption<T> option, T... alternatives) {
		final Integer choice = randomValueSupplier.apply(alternatives.length);
		T value = alternatives[choice];
		LOG.info("Randomly selected {} for {}", value, option.key());
		return value;
	}

	static PseudoRandomValueSelector create() {
		StackTraceElement entryPoint = findEntryPoint();
		int seed = Objects.hash(EnvironmentInformation.getGitCommitId(), entryPoint.getClassName(), entryPoint.getMethodName());
		return create(seed);
	}

	static PseudoRandomValueSelector create(int seed) {
		final Random random = new Random(seed);
		return new PseudoRandomValueSelector((Function<Integer, Integer> & Serializable) random::nextInt);
	}

	private static StackTraceElement findEntryPoint() {
		// find the entry point and use class + method name as seed components
		StackTraceElement entryPoint = null;
		final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
		for (int i = stackTrace.length - 1; i >= 0; i--) {
			entryPoint = stackTrace[i];
			if (entryPoint.getClassName().startsWith("org.apache.flink")) {
				break;
			}
		}
		assert entryPoint != null;
		return entryPoint;
	}
}
