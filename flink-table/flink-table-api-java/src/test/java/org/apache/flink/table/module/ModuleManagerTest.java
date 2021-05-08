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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.descriptors.CoreModuleDescriptorValidator.MODULE_TYPE_CORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ModuleManager}. */
public class ModuleManagerTest extends TestLogger {
    private ModuleManager manager;
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        manager = new ModuleManager();
    }

    @Test
    public void testLoadModuleTwice() {
        // CoreModule is loaded by default
        assertEquals(Collections.singletonList(MODULE_TYPE_CORE), manager.getUsedModules());
        assertEquals(CoreModule.INSTANCE, manager.getLoadedModules().get(MODULE_TYPE_CORE));

        thrown.expect(ValidationException.class);
        thrown.expectMessage("A module with name 'core' already exists");
        manager.loadModule(MODULE_TYPE_CORE, CoreModule.INSTANCE);
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
        expectedLoadedModules.put(MODULE_TYPE_CORE, CoreModule.INSTANCE);
        expectedLoadedModules.put("x", x);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);

        assertEquals(Arrays.asList(MODULE_TYPE_CORE, "x", "y", "z"), manager.getUsedModules());
        assertEquals(expectedLoadedModules, manager.getLoadedModules());
    }

    @Test
    public void testLoadModuleWithUnusedModulesExist() {
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule(y.getType(), y);
        manager.loadModule(z.getType(), z);

        Map<String, Module> expectedLoadedModules = new HashMap<>();
        expectedLoadedModules.put(MODULE_TYPE_CORE, CoreModule.INSTANCE);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);

        assertEquals(Arrays.asList(MODULE_TYPE_CORE, "y", "z"), manager.getUsedModules());
        assertEquals(expectedLoadedModules, manager.getLoadedModules());

        // disable module y and z
        manager.useModules(MODULE_TYPE_CORE);

        // load module x to test the order
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);
        expectedLoadedModules.put("x", x);

        assertEquals(Arrays.asList(MODULE_TYPE_CORE, "x"), manager.getUsedModules());
        assertEquals(expectedLoadedModules, manager.getLoadedModules());
    }

    @Test
    public void testUnloadModuleTwice() {
        assertEquals(Collections.singletonList(MODULE_TYPE_CORE), manager.getUsedModules());

        manager.unloadModule(MODULE_TYPE_CORE);
        assertEquals(Collections.emptyList(), manager.getUsedModules());
        assertEquals(Collections.emptyMap(), manager.getLoadedModules());

        thrown.expect(ValidationException.class);
        thrown.expectMessage("No module with name 'core' exists");
        manager.unloadModule(MODULE_TYPE_CORE);
    }

    @Test
    public void testUseUnloadedModules() {
        thrown.expect(ValidationException.class);
        thrown.expectMessage("No module with name 'x' exists");
        manager.useModules(MODULE_TYPE_CORE, "x");
    }

    @Test
    public void testUseModulesWithDuplicateModuleName() {
        thrown.expect(ValidationException.class);
        thrown.expectMessage("Module 'core' appears more than once");
        manager.useModules(MODULE_TYPE_CORE, MODULE_TYPE_CORE);
    }

    @Test
    public void testUseModules() {
        ModuleMock x = new ModuleMock("x");
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule(x.getType(), x);
        manager.loadModule(y.getType(), y);
        manager.loadModule(z.getType(), z);

        assertEquals(Arrays.asList(MODULE_TYPE_CORE, "x", "y", "z"), manager.getUsedModules());

        // test order for used modules
        manager.useModules("z", MODULE_TYPE_CORE);
        assertEquals(Arrays.asList("z", MODULE_TYPE_CORE), manager.getUsedModules());

        // test unmentioned modules are still loaded
        Map<String, Module> expectedLoadedModules = new HashMap<>();
        expectedLoadedModules.put(MODULE_TYPE_CORE, CoreModule.INSTANCE);
        expectedLoadedModules.put("x", x);
        expectedLoadedModules.put("y", y);
        expectedLoadedModules.put("z", z);
        assertEquals(expectedLoadedModules, manager.getLoadedModules());
    }

    @Test
    public void testListModules() {
        ModuleMock y = new ModuleMock("y");
        ModuleMock z = new ModuleMock("z");
        manager.loadModule("y", y);
        manager.loadModule("z", z);
        manager.useModules("z", "y");

        assertEquals(Arrays.asList("z", "y"), manager.listModules());
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

        assertEquals(
                getExpectedModuleEntries(2, "z", "y", MODULE_TYPE_CORE, "x"),
                manager.listFullModules());
    }

    @Test
    public void testListFunctions() {
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);

        assertTrue(manager.listFunctions().contains("dummy"));

        // should not return function name of an unused module
        manager.useModules(MODULE_TYPE_CORE);
        assertFalse(manager.listFunctions().contains("dummy"));
    }

    @Test
    public void testGetFunctionDefinition() {
        ModuleMock x = new ModuleMock("x");
        manager.loadModule(x.getType(), x);

        assertTrue(manager.getFunctionDefinition("dummy").isPresent());

        // should not return function definition of an unused module
        manager.useModules(MODULE_TYPE_CORE);
        assertFalse(manager.getFunctionDefinition("dummy").isPresent());
    }

    private static List<ModuleEntry> getExpectedModuleEntries(int index, String... names) {
        return IntStream.range(0, names.length)
                .mapToObj(i -> new ModuleEntry(names[i], i < index))
                .collect(Collectors.toList());
    }
}
