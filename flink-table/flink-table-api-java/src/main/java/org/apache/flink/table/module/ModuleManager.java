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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
 * Responsible for loading/unloading modules, managing their life cycles, and resolving module
 * objects.
 */
public class ModuleManager {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleManager.class);

    /** To keep {@link #listFullModules()} deterministic. */
    private LinkedHashMap<String, Module> loadedModules;

    /** Keep tracking used modules with resolution order. */
    private List<String> usedModules;

    public ModuleManager() {
        this.loadedModules = new LinkedHashMap<>();
        this.usedModules = new ArrayList<>();
        loadedModules.put(MODULE_TYPE_CORE, CoreModule.INSTANCE);
        usedModules.add(MODULE_TYPE_CORE);
    }

    /**
     * Load a module under a unique name. Modules will be kept in the loaded order, and new module
     * will be added to the left before the unused module and turn on use by default.
     *
     * @param name name of the module
     * @param module the module instance
     * @throws ValidationException when there already exists a module with the same name
     */
    public void loadModule(String name, Module module) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty string");
        checkNotNull(module, "module cannot be null");

        if (loadedModules.containsKey(name)) {
            throw new ValidationException(
                    String.format("A module with name '%s' already exists", name));
        } else {
            usedModules.add(name);
            loadedModules.put(name, module);
            LOG.info("Loaded module '{}' from class {}", name, module.getClass().getName());
        }
    }

    /**
     * Unload a module with given name.
     *
     * @param name name of the module
     * @throws ValidationException when there is no module with the given name
     */
    public void unloadModule(String name) {
        if (loadedModules.containsKey(name)) {
            loadedModules.remove(name);
            boolean used = usedModules.remove(name);
            LOG.info("Unloaded an {} module '{}'", used ? "used" : "unused", name);
        } else {
            throw new ValidationException(String.format("No module with name '%s' exists", name));
        }
    }

    /**
     * Enable modules in use with declared name order. Modules that have been loaded but not exist
     * in names varargs will become unused.
     *
     * @param names module names to be used
     * @throws ValidationException when module names contain an unloaded name
     */
    public void useModules(String... names) {
        checkNotNull(names, "names cannot be null");
        Set<String> deduplicateNames = new HashSet<>();
        for (String name : names) {
            if (!loadedModules.containsKey(name)) {
                throw new ValidationException(
                        String.format("No module with name '%s' exists", name));
            }
            if (!deduplicateNames.add(name)) {
                throw new ValidationException(
                        String.format("Module '%s' appears more than once", name));
            }
        }
        usedModules.clear();
        usedModules.addAll(Arrays.asList(names));
    }

    /**
     * Get names of all used modules in resolution order.
     *
     * @return a list of names of used modules
     */
    public List<String> listModules() {
        return new ArrayList<>(usedModules);
    }

    /**
     * Get all loaded modules with use status. Modules in use status are returned in resolution
     * order.
     *
     * @return a list of module entries with module name and use status
     */
    public List<ModuleEntry> listFullModules() {
        // keep the order for used modules
        List<ModuleEntry> moduleEntries =
                usedModules.stream()
                        .map(name -> new ModuleEntry(name, true))
                        .collect(Collectors.toList());
        loadedModules.keySet().stream()
                .filter(name -> !usedModules.contains(name))
                .forEach(name -> moduleEntries.add(new ModuleEntry(name, false)));
        return moduleEntries;
    }

    /**
     * Get names of all functions from used modules.
     *
     * @return a set of function names of used modules
     */
    public Set<String> listFunctions() {
        return usedModules.stream()
                .map(name -> loadedModules.get(name).listFunctions())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Get an optional of {@link FunctionDefinition} by a given name. Function will be resolved to
     * modules in the used order, and the first match will be returned. If no match is found in all
     * modules, return an optional.
     *
     * @param name name of the function
     * @return an optional of {@link FunctionDefinition}
     */
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        for (String moduleName : usedModules) {
            if (loadedModules.get(moduleName).listFunctions().stream()
                    .anyMatch(name::equalsIgnoreCase)) {
                LOG.debug("Got FunctionDefinition '{}' from '{}' module.", name, moduleName);
                return loadedModules.get(moduleName).getFunctionDefinition(name);
            }
        }

        LOG.debug("Cannot find FunctionDefinition '{}' from any loaded modules.", name);
        return Optional.empty();
    }

    @VisibleForTesting
    List<String> getUsedModules() {
        return usedModules;
    }

    @VisibleForTesting
    Map<String, Module> getLoadedModules() {
        return loadedModules;
    }
}
