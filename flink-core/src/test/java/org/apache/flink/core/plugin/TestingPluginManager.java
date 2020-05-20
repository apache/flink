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

package org.apache.flink.core.plugin;

import org.apache.commons.collections.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * {@link PluginManager} for testing purpose.
 */
public class TestingPluginManager implements PluginManager {

	private final Map<Class<?>, Iterator<?>> plugins;

	public TestingPluginManager(Map<Class<?>, Iterator<?>> plugins) {
		this.plugins = plugins;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <P> Iterator<P> load(Class<P> service) {
		return (Iterator<P>) plugins.getOrDefault(service, IteratorUtils.emptyIterator());
	}
}
