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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.CoreModuleDescriptorValidator.MODULE_TYPE_CORE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Responsible for loading/unloading modules, managing their life cycles, and resolving module objects.
 */
public class ModuleManager {

	private static final Logger LOG = LoggerFactory.getLogger(ModuleManager.class);

	private LinkedHashMap<String, Module> modules;

	public ModuleManager() {
		this.modules = new LinkedHashMap<>();

		modules.put(MODULE_TYPE_CORE, CoreModule.INSTANCE);
	}

	/**
	 * Load a module under a unique name. Modules will be kept in the loaded order, and new module
	 * will be added to the end.
	 * ValidationException is thrown when there is already a module with the same name.
	 *
	 * @param name name of the module
	 * @param module the module instance
	 */
	public void loadModule(String name, Module module) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty string");
		checkNotNull(module, "module cannot be null");

		if (!modules.containsKey(name)) {
			modules.put(name, module);

			LOG.info("Loaded module {} from class {}", name, module.getClass().getName());
		} else {
			throw new ValidationException(
				String.format("A module with name %s already exists", name));
		}
	}

	/**
	 * Unload a module with given name.
	 * ValidationException is thrown when there is no module with the given name.
	 *
	 * @param name name of the module
	 */
	public void unloadModule(String name) {
		if (modules.containsKey(name)) {
			modules.remove(name);

			LOG.info("Unloaded module {}", name);
		} else {
			throw new ValidationException(
				String.format("No module with name %s exists", name));
		}
	}

	/**
	 * Get names of all modules loaded.
	 *
	 * @return a list of names of modules loaded
	 */
	public List<String> listModules() {
		return new ArrayList<>(modules.keySet());
	}

	/**
	 * Get names of all functions from all modules.
	 *
	 * @return a set of names of registered modules.
	 */
	public Set<String> listFunctions() {
		return modules.values().stream()
				.map(m -> m.listFunctions())
				.flatMap(n -> n.stream())
				.collect(Collectors.toSet());
	}

	/**
	 * Get an optional of {@link FunctionDefinition} by a given name.
	 * Function will be resolved to modules in the loaded order, and the first match will be returned.
	 * If no match is found in all modules, return an optional.
	 *
	 * @param name name of the function
	 * @return an optional of {@link FunctionDefinition}
	 */
	public Optional<FunctionDefinition> getFunctionDefinition(String name) {
		Optional<Map.Entry<String, Module>> result = modules.entrySet().stream()
			.filter(p -> p.getValue().listFunctions().stream().anyMatch(e -> e.equalsIgnoreCase(name)))
			.findFirst();

		if (result.isPresent()) {
			LOG.info("Got FunctionDefinition '{}' from '{}' module.", name, result.get().getKey());

			return result.get().getValue().getFunctionDefinition(name);
		} else {
			LOG.debug("Cannot find FunctionDefinition '{}' from any loaded modules.", name);

			return Optional.empty();
		}
	}

}
