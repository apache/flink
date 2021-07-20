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

package org.apache.flink.table.client.config.entries;

import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.descriptors.FunctionDescriptorValidator;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a user-defined function configuration entry.
 *
 * @deprecated This will be removed in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class FunctionEntry extends ConfigEntry {

    public static final String FUNCTIONS_NAME = "name";

    private String name;

    private FunctionEntry(String name, DescriptorProperties properties) {
        super(properties);
        this.name = name;
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        new FunctionDescriptorValidator().validate(properties);
    }

    public String getName() {
        return name;
    }

    public FunctionDescriptor getDescriptor() {
        return new FunctionEntryDescriptor();
    }

    public static FunctionEntry create(Map<String, Object> config) {
        return create(ConfigUtil.normalizeYaml(config));
    }

    private static FunctionEntry create(DescriptorProperties properties) {
        properties.validateString(FUNCTIONS_NAME, false, 1);

        final String name = properties.getString(FUNCTIONS_NAME);

        final DescriptorProperties cleanedProperties =
                properties.withoutKeys(Collections.singletonList(FUNCTIONS_NAME));

        return new FunctionEntry(name, cleanedProperties);
    }

    // --------------------------------------------------------------------------------------------

    private class FunctionEntryDescriptor extends FunctionDescriptor {

        @Override
        public Map<String, String> toProperties() {
            return FunctionEntry.this.properties.asMap();
        }
    }
}
