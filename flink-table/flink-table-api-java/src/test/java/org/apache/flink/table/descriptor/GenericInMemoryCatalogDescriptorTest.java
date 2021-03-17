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

package org.apache.flink.table.descriptor;

import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;
import org.apache.flink.table.descriptors.GenericInMemoryCatalogDescriptor;
import org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for the {@link GenericInMemoryCatalogDescriptor} descriptor. */
public class GenericInMemoryCatalogDescriptorTest extends DescriptorTestBase {

    private static final String TEST_DATABASE = "test";

    @Override
    protected List<Descriptor> descriptors() {
        final Descriptor withoutDefaultDB = new GenericInMemoryCatalogDescriptor();

        final Descriptor withDefaultDB = new GenericInMemoryCatalogDescriptor(TEST_DATABASE);

        return Arrays.asList(withoutDefaultDB, withDefaultDB);
    }

    @Override
    protected List<Map<String, String>> properties() {
        final Map<String, String> props1 = new HashMap<>();
        props1.put("type", "generic_in_memory");
        props1.put("property-version", "1");

        final Map<String, String> props2 = new HashMap<>();
        props2.put("type", "generic_in_memory");
        props2.put("property-version", "1");
        props2.put("default-database", TEST_DATABASE);

        return Arrays.asList(props1, props2);
    }

    @Override
    protected DescriptorValidator validator() {
        return new GenericInMemoryCatalogValidator();
    }
}
