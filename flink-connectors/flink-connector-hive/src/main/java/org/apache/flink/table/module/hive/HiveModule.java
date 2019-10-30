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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_1_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_1_1;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Module to provide Hive built-in metadata.
 */
public class HiveModule implements Module {

	private final HiveFunctionDefinitionFactory factory;
	private final HiveShim hiveShim;

	public HiveModule(String hiveVersion) {
		// Fail early to warn users
		// Should be re-evaluated if metadata other than Hive built-in functions are supported
		checkArgument(!(hiveVersion.equals(HIVE_VERSION_V1_1_0) || hiveVersion.equals(HIVE_VERSION_V1_1_1)),
			"HiveModule is not supported for versions prior to 1.2.0, " +
				"since Hive cannot list built-in functions before 1.2.0.");

		this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		this.factory = new HiveFunctionDefinitionFactory(hiveShim);
	}

	@Override
	public Set<String> listFunctions() {
		return hiveShim.listBuiltInFunctions();
	}

	@Override
	public Optional<FunctionDefinition> getFunctionDefinition(String name) {
		FunctionInfo info = getFunctionInfo(name);

		if (info.isBuiltIn()) {
			return Optional.of(factory.createFunctionDefinitionFromHiveFunction(name, info.getFunctionClass().getName()));
		} else {
			return Optional.empty();
		}
	}

	private FunctionInfo getFunctionInfo(String name) {
		try {
			return FunctionRegistry.getFunctionInfo(name);
		} catch (SemanticException e) {
			throw new FlinkHiveException(
				String.format("Failed getting function info for %s", name), e);
		}
	}
}
