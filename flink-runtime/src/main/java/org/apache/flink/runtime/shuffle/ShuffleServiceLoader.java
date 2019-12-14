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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS;

/**
 * Utility to load the pluggable {@link ShuffleServiceFactory} implementations.
 */
public enum ShuffleServiceLoader {
	;

	public static ShuffleServiceFactory<?, ?, ?> loadShuffleServiceFactory(Configuration configuration) throws FlinkException {
		String shuffleServiceClassName = configuration.getString(SHUFFLE_SERVICE_FACTORY_CLASS);
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		return InstantiationUtil.instantiate(
			shuffleServiceClassName,
			ShuffleServiceFactory.class,
			classLoader);
	}
}
