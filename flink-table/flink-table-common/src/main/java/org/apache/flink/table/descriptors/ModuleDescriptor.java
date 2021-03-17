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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Describes a {@link org.apache.flink.table.module.Module}. */
@PublicEvolving
public abstract class ModuleDescriptor extends DescriptorBase {

    private final String type;

    /**
     * Constructs a {@link ModuleDescriptor}.
     *
     * @param type string that identifies this catalog
     */
    public ModuleDescriptor(String type) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(type), "type cannot be null or empty");

        this.type = type;
    }

    @Override
    public final Map<String, String> toProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putString(MODULE_TYPE, type);

        properties.putProperties(toModuleProperties());
        return properties.asMap();
    }

    /** Converts this descriptor into a set of module properties. */
    protected abstract Map<String, String> toModuleProperties();
}
