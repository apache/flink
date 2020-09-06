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

package org.apache.flink.table.module.hive;

import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.apache.flink.table.module.hive.HiveModuleDescriptorValidator.MODULE_HIVE_VERSION;
import static org.apache.flink.table.module.hive.HiveModuleDescriptorValidator.MODULE_TYPE_HIVE;

/**
 * Factory for {@link HiveModule}.
 */
public class HiveModuleFactory implements ModuleFactory {

	@Override
	public Module createModule(Map<String, String> properties) {
		final DescriptorProperties descProperties = getValidatedProperties(properties);

		final String hiveVersion = descProperties.getOptionalString(MODULE_HIVE_VERSION)
			.orElse(HiveShimLoader.getHiveVersion());

		return new HiveModule(hiveVersion);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new HiveModuleDescriptorValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(MODULE_TYPE, MODULE_TYPE_HIVE);

		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Arrays.asList(MODULE_HIVE_VERSION);
	}
}
