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

package org.apache.flink.table.factories;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.factories.TestTableSinkFactory.CONNECTOR_TYPE_VALUE_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.FORMAT_PATH;
import static org.apache.flink.table.factories.TestTableSinkFactory.FORMAT_TYPE_VALUE_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.REQUIRED_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.REQUIRED_TEST_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for testing table sink discovery using {@link TableFactoryService}. The tests assume the
 * table sink factory {@link TestTableSinkFactory} is registered.
 */
class TableSinkFactoryServiceTest {

    @Test
    void testValidProperties() {
        Map<String, String> props = properties();
        assertThat(TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(TestTableSinkFactory.class);
    }

    @Test
    void testInvalidContext() {
        Map<String, String> props = properties();
        props.put(CONNECTOR_TYPE, "unknown-connector-type");
        assertThatThrownBy(() -> TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(NoMatchingTableFactoryException.class)
                .hasMessageContaining("Could not find a suitable table factory");
    }

    @Test
    void testDifferentContextVersion() {
        Map<String, String> props = properties();
        props.put(CONNECTOR_PROPERTY_VERSION, "2");
        // the table source should still be found
        assertThat(TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(TestTableSinkFactory.class);
    }

    @Test
    void testUnsupportedProperty() {
        Map<String, String> props = properties();
        props.put("format.path_new", "/new/path");
        assertThatThrownBy(() -> TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(NoMatchingTableFactoryException.class)
                .hasMessageContaining(
                        "The matching candidates:\n"
                                + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                                + "Unsupported property keys:\n"
                                + "format.path_new");
    }

    @Test
    void testMissingProperty() {
        Map<String, String> props = properties();
        props.remove(TableFactoryService.FORMAT_TYPE);
        assertThatThrownBy(() -> TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(NoMatchingTableFactoryException.class)
                .hasMessageContaining(
                        "The matching candidates:\n"
                                + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                                + "Missing properties:\n"
                                + "format.type=test");
    }

    @Test
    void testMismatchedProperty() {
        Map<String, String> props = properties();
        props.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST + "_new");
        assertThatThrownBy(() -> TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(NoMatchingTableFactoryException.class)
                .hasMessageContaining(
                        "The matching candidates:\n"
                                + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                                + "Mismatched properties:\n"
                                + "'format.type' expects 'test', but is 'test_new'");
    }

    @Test
    void testMissingAndMismatchedProperty() {
        Map<String, String> props = properties();
        props.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST + "_new");
        props.remove(REQUIRED_TEST);
        assertThatThrownBy(() -> TableFactoryService.find(TableSinkFactory.class, props))
                .isInstanceOf(NoMatchingTableFactoryException.class)
                .hasMessageContaining(
                        "The matching candidates:\n"
                                + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                                + "Missing properties:\n"
                                + "required.test=required-0\n"
                                + "Mismatched properties:\n"
                                + "'format.type' expects 'test', but is 'test_new'");
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TEST);
        properties.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST);
        properties.put(REQUIRED_TEST, REQUIRED_TEST_VALUE);
        properties.put(CONNECTOR_PROPERTY_VERSION, "1");
        properties.put(TableFactoryService.FORMAT_PROPERTY_VERSION, "1");
        properties.put(FORMAT_PATH, "/path/to/target");
        properties.put("schema.0.name", "a");
        properties.put("schema.1.name", "b");
        properties.put("schema.2.name", "c");
        properties.put("schema.0.field.0.name", "a");
        properties.put("schema.0.field.1.name", "b");
        properties.put("schema.0.field.2.name", "c");
        return properties;
    }
}
