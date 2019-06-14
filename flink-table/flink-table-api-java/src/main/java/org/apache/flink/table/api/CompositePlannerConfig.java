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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link PlannerConfig} to pass multiple different configs under a single object. It allows storing only a single
 * instance of a given class. If there is an object of the same class already, it is replaced with the new one.
 */
@Internal
public class CompositePlannerConfig implements PlannerConfig {
	private final Map<Class<? extends PlannerConfig>, PlannerConfig> configs = new HashMap<>();

	public void addConfig(PlannerConfig config) {
		configs.put(config.getClass(), config);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends PlannerConfig> Optional<T> unwrap(Class<T> type) {
		return (Optional<T>) Optional.ofNullable(configs.get(type));
	}
}
