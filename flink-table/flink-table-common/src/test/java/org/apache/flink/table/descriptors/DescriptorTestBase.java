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

import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Test base for testing {@link Descriptor} together with {@link DescriptorValidator}. */
public abstract class DescriptorTestBase {

    /** Returns a set of valid descriptors. */
    protected abstract List<Descriptor> descriptors();

    /** Returns a set of properties for each valid descriptor. */
    protected abstract List<Map<String, String>> properties();

    /** Returns a validator that can validate all valid descriptors. */
    protected abstract DescriptorValidator validator();

    @Test
    public void testValidation() {
        final List<Descriptor> descriptors = descriptors();
        final List<Map<String, String>> properties = properties();

        Preconditions.checkArgument(descriptors.size() == properties.size());

        for (int i = 0; i < descriptors.size(); i++) {
            verifyProperties(descriptors.get(i), properties.get(i));
        }
    }

    protected void verifyProperties(Descriptor descriptor, Map<String, String> expected) {
        // test produced properties
        assertEquals(expected, descriptor.toProperties());

        // test validation logic
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(expected);
        validator().validate(properties);
    }

    protected void addPropertyAndVerify(Descriptor descriptor, String property, String newValue) {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(descriptor.toProperties());
        final DescriptorProperties copy =
                properties.withoutKeys(Collections.singletonList(property));
        copy.putString(property, newValue);
        validator().validate(copy);
    }

    protected void removePropertyAndVerify(Descriptor descriptor, String property) {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(descriptor.toProperties());
        final DescriptorProperties copy =
                properties.withoutKeys(Collections.singletonList(property));
        validator().validate(copy);
    }
}
