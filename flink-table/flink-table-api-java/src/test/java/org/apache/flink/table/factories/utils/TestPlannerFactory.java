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

package org.apache.flink.table.factories.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test {@link Planner} factory used in {@link
 * org.apache.flink.table.factories.ComponentFactoryServiceTest}.
 */
public class TestPlannerFactory implements PlannerFactory {

    public static final String PLANNER_TYPE_KEY = "planner-type";
    public static final String PLANNER_TYPE_VALUE = "test-planner";

    @Override
    public Planner create(
            Map<String, String> properties,
            Executor executor,
            TableConfig tableConfig,
            FunctionCatalog functionCatalog,
            CatalogManager catalogManager) {
        return null;
    }

    @Override
    public Map<String, String> optionalContext() {
        HashMap<String, String> map = new HashMap<>();
        map.put(EnvironmentSettings.CLASS_NAME, this.getClass().getCanonicalName());
        return map;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> map = new HashMap<>();
        map.put(PLANNER_TYPE_KEY, PLANNER_TYPE_VALUE);
        return map;
    }

    @Override
    public List<String> supportedProperties() {
        return Arrays.asList(EnvironmentSettings.CLASS_NAME, EnvironmentSettings.STREAMING_MODE);
    }
}
