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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.table.module.hive.HiveModuleOptions.HIVE_VERSION;

/** Factory for {@link HiveModule}. */
@Internal
public class HiveModuleFactory implements ModuleFactory {

    public static final String IDENTIFIER = "hive";

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
        return Collections.singleton(HIVE_VERSION);
    }

    @Override
    public Module createModule(Context context) {
        final FactoryUtil.ModuleFactoryHelper factoryHelper =
                FactoryUtil.createModuleFactoryHelper(this, context);
        factoryHelper.validate();

        final String hiveVersion =
                factoryHelper
                        .getOptions()
                        .getOptional(HIVE_VERSION)
                        .orElseGet(HiveShimLoader::getHiveVersion);

        return new HiveModule(hiveVersion, context.getConfiguration(), context.getClassLoader());
    }
}
