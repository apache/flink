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
import org.apache.flink.table.utils.ModuleMock;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ModuleManager}. */
public class ModuleManagerTest extends TestLogger {

    private ModuleManager manager;

    @Before
    public void before() {
        manager = new ModuleManager();
    }

    @Test
    public void testLoadModuleTwice() {
        // CoreModule is loaded by default
        assertThat(manager.getUsedModules())
                .isEqualTo(Collections.singletonList(CoreModuleFactory.IDENTIFIER));
        assertThat(manager.getLoadedModules().get(CoreModuleFactory.IDENTIFIER))
                .isSameAs(CoreModule.INSTANCE);

        assertThatThrownBy(
                        () -> manager.loadModule(CoreModuleFactory.IDENTIFIER, CoreModule.INSTANCE))
                .isInstanceOf(ValidationException.class)
                .hasMessage("A module with name 'core' already exists");
    }

    @Test
    public void testLoadModuleWithoutUnusedModulesExist() {
        ModuleMock x = new ModuleMock("x");
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule(x.getType(), x);
        manager.loadModule(y.getType(), y);
        manager.loadModule(z.getType(), z);

        Map<String, Module> expectedLoadedModules = new HashMap<>();
        expectedLoadedModules.put(CoreModuleFactory.IDENTIFIER, CoreModule.INSTANCE);
        expectedLoadedModules.put("x", x);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);

        assertThat(manager.getUsedModules())
                .containsSequence(CoreModuleFactory.IDENTIFIER, "x", "y", "z");
        assertThat(manager.getLoadedModules()).isEqualTo(expectedLoadedModules);
    }

    @Test
    public void testLoadModuleWithUnusedModulesExist() {
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule(y.getType(), y);
        manager.loadModule(z.getType(), z);

        Map<String, Module> expectedLoadedModules = new HashMap<>();
        expectedLoadedModules.put(CoreModuleFactory.IDENTIFIER, CoreModule.INSTANCE);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);

        assertThat(manager.getUsedModules())
                .containsSequence(CoreModuleFactory.IDENTIFIER, "y", "z");
        assertThat(manager.getLoadedModules()).isEqualTo(expectedLoadedModules);

        // disable module y and z
        manager.useModules(CoreModuleFactory.IDENTIFIER);

        // load module x to test the order
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);
        expectedLoadedModules.put("x", x);

        assertThat(manager.getUsedModules()).containsSequence(CoreModuleFactory.IDENTIFIER, "x");
        assertThat(manager.getLoadedModules()).isEqualTo(expectedLoadedModules);
    }

    @Test
    public void testUnloadModuleTwice() {
        assertThat(manager.getUsedModules()).containsSequence(CoreModuleFactory.IDENTIFIER);

        manager.unloadModule(CoreModuleFactory.IDENTIFIER);
        assertThat(manager.getUsedModules()).isEmpty();
        assertThat(manager.getLoadedModules()).isEmpty();

        assertThatThrownBy(() -> manager.unloadModule(CoreModuleFactory.IDENTIFIER))
                .isInstanceOf(ValidationException.class)
                .hasMessage("No module with name 'core' exists");
    }

    @Test
    public void testUseUnloadedModules() {
        assertThatThrownBy(() -> manager.useModules(CoreModuleFactory.IDENTIFIER, "x"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("No module with name 'x' exists");
    }

    @Test
    public void testUseModulesWithDuplicateModuleName() {
        assertThatThrownBy(
                        () ->
                                manager.useModules(
                                        CoreModuleFactory.IDENTIFIER, CoreModuleFactory.IDENTIFIER))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Module 'core' appears more than once");
    }

    @Test
    public void testUseModules() {
        ModuleMock x = new ModuleMock("x");
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule(x.getType(), x);
        manager.loadModule(y.getType(), y);
        manager.loadModule(z.getType(), z);

        assertThat(manager.getUsedModules())
                .containsSequence(CoreModuleFactory.IDENTIFIER, "x", "y", "z");

        // test order for used modules
        manager.useModules("z", CoreModuleFactory.IDENTIFIER);
        assertThat(manager.getUsedModules()).containsSequence("z", CoreModuleFactory.IDENTIFIER);

        // test unmentioned modules are still loaded
        Map<String, Module> expectedLoadedModules = new HashMap<>();
        expectedLoadedModules.put(CoreModuleFactory.IDENTIFIER, CoreModule.INSTANCE);
        expectedLoadedModules.put("x", x);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);
        assertThat(manager.getLoadedModules()).isEqualTo(expectedLoadedModules);
    }

    @Test
    public void testListModules() {
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule("y", y);
        manager.loadModule("z", z);
        manager.useModules("z", "y");

        assertThat(manager.listModules()).containsSequence("z", "y");
    }

    @Test
    public void testListFullModules() {
        ModuleMock x = new ModuleMock("x");
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");

        manager.loadModule("y", y);
        manager.loadModule("x", x);
        manager.loadModule("z", z);
        manager.useModules("z", "y");

        assertThat(manager.listFullModules())
                .isEqualTo(
                        getExpectedModuleEntries(2, "z", "y", CoreModuleFactory.IDENTIFIER, "x"));
    }

    @Test
    public void testListFunctions() {
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);

        assertThat(manager.listFunctions()).contains(ModuleMock.DUMMY_FUNCTION_NAME);

        // hidden functions not in the default list
        assertThat(manager.listFunctions()).doesNotContain(ModuleMock.INTERNAL_FUNCTION_NAME);

        // should not return function name of an unused module
        manager.useModules(CoreModuleFactory.IDENTIFIER);
        assertThat(manager.listFunctions()).doesNotContain(ModuleMock.DUMMY_FUNCTION_NAME);
    }

    @Test
    public void testGetFunctionDefinition() {
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);

        assertThat(manager.getFunctionDefinition(ModuleMock.DUMMY_FUNCTION_NAME)).isPresent();

        assertThat(manager.getFunctionDefinition(ModuleMock.INTERNAL_FUNCTION_NAME)).isPresent();

        // should not return function definition of an unused module
        manager.useModules(CoreModuleFactory.IDENTIFIER);
        assertThat(manager.getFunctionDefinition(ModuleMock.DUMMY_FUNCTION_NAME)).isEmpty();
    }

    private static List<ModuleEntry> getExpectedModuleEntries(int index, String... names) {
        return IntStream.range(0, names.length)
                .mapToObj(i -> new ModuleEntry(names[i], i < index))
                .collect(Collectors.toList());
    }
}
