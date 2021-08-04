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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.ModuleFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;

/** Factory for {@link CoreModule}. */
@Internal
public class CoreModuleFactory implements ModuleFactory {

    public static final String IDENTIFIER = "core";

    @Override
    public Module createModule(Map<String, String> properties) {
        return CoreModule.INSTANCE;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(MODULE_TYPE, IDENTIFIER);

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return new ArrayList<>();
    }
}
