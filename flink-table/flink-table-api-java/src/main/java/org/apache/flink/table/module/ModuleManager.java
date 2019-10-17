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

package org.apache.flink.table.module;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.exceptions.ModuleAlreadyExistException;
import org.apache.flink.table.module.exceptions.ModuleNotFoundException;
import org.apache.flink.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Responsible for loading/unloading modules, managing their life cycles, and resolving module objects.
 */
public class ModuleManager {
	private LinkedHashMap<String, Module> modules;

	public ModuleManager() {
		this.modules = new LinkedHashMap<>();

		// TODO: Add Core module to modules
	}

	public void loadModule(String name, Module module) throws ModuleAlreadyExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty string");
		checkNotNull(module, "module cannot be null");

		if (!modules.containsKey(name)) {
			modules.put(name, module);
		} else {
			throw new ModuleAlreadyExistException(name);
		}
	}

	public void unloadModule(String name) throws ModuleNotFoundException {
		if (modules.containsValue(name)) {
			modules.remove(name);
		} else {
			throw new ModuleNotFoundException(name);
		}
	}

	public Set<String> listFunctions() {
		return modules.values().stream()
				.map(m -> m.listFunctions())
				.flatMap(n -> n.stream())
				.collect(Collectors.toSet());
	}

	public Optional<FunctionDefinition> getFunctionDefinition(String normalizedName) {
		Optional<Module> module = modules.values().stream()
			.filter(p -> p.listFunctions().stream().anyMatch(e -> e.equals(normalizedName)))
			.findFirst();

		return module.isPresent() ? module.get().getFunctionDefinition(normalizedName) : Optional.empty();
	}

}
