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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The default implementation of the {@link ExecutorServiceLoader}. This implementation uses
 * Java service discovery to find the available {@link ExecutorFactory executor factories}.
 */
@Internal
public class DefaultExecutorServiceLoader implements ExecutorServiceLoader {

	// TODO: This code is almost identical to the ClusterClientServiceLoader and its default implementation.
	// The reason of this duplication is the package structure which does not allow for the ExecutorServiceLoader
	// to know about the ClusterClientServiceLoader. Remove duplication when package structure has improved.

	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutorServiceLoader.class);

	private static final ServiceLoader<ExecutorFactory> defaultLoader = ServiceLoader.load(ExecutorFactory.class);

	public static final DefaultExecutorServiceLoader INSTANCE = new DefaultExecutorServiceLoader();

	@Override
	public ExecutorFactory getExecutorFactory(final Configuration configuration) {
		checkNotNull(configuration);

		final List<ExecutorFactory> compatibleFactories = new ArrayList<>();
		final Iterator<ExecutorFactory> factories = defaultLoader.iterator();
		while (factories.hasNext()) {
			try {
				final ExecutorFactory factory = factories.next();
				if (factory != null && factory.isCompatibleWith(configuration)) {
					compatibleFactories.add(factory);
				}
			} catch (Throwable e) {
				if (e.getCause() instanceof NoClassDefFoundError) {
					LOG.info("Could not load factory due to missing dependencies.");
				} else {
					throw e;
				}
			}
		}

		if (compatibleFactories.size() > 1) {
			final String configStr =
					configuration.toMap().entrySet().stream()
							.map(e -> e.getKey() + "=" + e.getValue())
							.collect(Collectors.joining("\n"));

			throw new IllegalStateException("Multiple compatible client factories found for:\n" + configStr + ".");
		}

		return compatibleFactories.isEmpty() ? null : compatibleFactories.get(0);
	}

	private DefaultExecutorServiceLoader() {
		// make sure nobody instantiates us explicitly.
	}
}
