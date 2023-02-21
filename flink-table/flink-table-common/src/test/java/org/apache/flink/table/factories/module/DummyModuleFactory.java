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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.Set;

/** Test implementation for {@link ModuleFactory}. */
public class DummyModuleFactory implements ModuleFactory {

    public static final String IDENTIFIER = "dummy";

    private static final ConfigOption<String> DUMMY_VERSION =
            ConfigOptions.key("dummy-version").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(DUMMY_VERSION);
    }

    @Override
    public Module createModule(Context context) {
        return new Module() {};
    }
}
