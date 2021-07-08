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

package org.apache.flink.table.factories.module;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ModuleDescriptorValidator;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;

/** Test implementation for {@link ModuleFactory}. */
public class DummyModuleFactory implements ModuleFactory {

    @Override
    public Module createModule(Map<String, String> properties) {
        return new Module() {};
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(MODULE_TYPE, DummyModuleDescriptorValidator.MODULE_TYPE_DUMMY);

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList(DummyModuleDescriptorValidator.MODULE_DUMMY_VERSION);
    }

    /** Test implementation for {@link ModuleDescriptorValidator}. */
    public static class DummyModuleDescriptorValidator extends ModuleDescriptorValidator {
        public static final String MODULE_TYPE_DUMMY = "dummy";
        public static final String MODULE_DUMMY_VERSION = "dummy-version";

        @Override
        public void validate(DescriptorProperties properties) {
            super.validate(properties);
            properties.validateValue(MODULE_TYPE, MODULE_TYPE_DUMMY, false);
            properties.validateString(MODULE_DUMMY_VERSION, true, 1);
        }
    }
}
