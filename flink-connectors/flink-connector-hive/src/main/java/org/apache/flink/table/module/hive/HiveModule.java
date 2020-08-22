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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Module to provide Hive built-in metadata.
 */
public class HiveModule implements Module {

	// a set of functions that shouldn't be overridden by HiveModule
	@VisibleForTesting
	static final Set<String> BUILT_IN_FUNC_BLACKLIST = Collections.unmodifiableSet(new HashSet<>(
			Arrays.asList("count", "current_date", "current_timestamp", "dense_rank", "first_value", "lag", "last_value",
					"lead", "rank", "row_number", "hop", "hop_end", "hop_proctime", "hop_rowtime", "hop_start",
					"session", "session_end", "session_proctime", "session_rowtime", "session_start",
					"tumble", "tumble_end", "tumble_proctime", "tumble_rowtime", "tumble_start")));

	private final HiveFunctionDefinitionFactory factory;
	private final String hiveVersion;
	private final HiveShim hiveShim;

	public HiveModule() {
		this(HiveShimLoader.getHiveVersion());
	}

	public HiveModule(String hiveVersion) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveVersion), "hiveVersion cannot be null");

		this.hiveVersion = hiveVersion;
		this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		this.factory = new HiveFunctionDefinitionFactory(hiveShim);
	}

	@Override
	public Set<String> listFunctions() {
		Set<String> builtInFuncs = hiveShim.listBuiltInFunctions();
		builtInFuncs.removeAll(BUILT_IN_FUNC_BLACKLIST);
		return builtInFuncs;
	}

	@Override
	public Optional<FunctionDefinition> getFunctionDefinition(String name) {
		if (BUILT_IN_FUNC_BLACKLIST.contains(name)) {
			return Optional.empty();
		}
		Optional<FunctionInfo> info = hiveShim.getBuiltInFunctionInfo(name);

		return info.map(functionInfo -> factory.createFunctionDefinitionFromHiveFunction(name, functionInfo.getFunctionClass().getName()));
	}

	public String getHiveVersion() {
		return hiveVersion;
	}
}
