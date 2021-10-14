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

import org.apache.flink.table.api.NoMatchingTableFactoryException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.factories.TestTableSinkFactory.CONNECTOR_TYPE_VALUE_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.FORMAT_PATH;
import static org.apache.flink.table.factories.TestTableSinkFactory.FORMAT_TYPE_VALUE_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.REQUIRED_TEST;
import static org.apache.flink.table.factories.TestTableSinkFactory.REQUIRED_TEST_VALUE;
import static org.junit.Assert.assertTrue;

/**
 * Tests for testing table sink discovery using {@link TableFactoryService}. The tests assume the
 * table sink factory {@link TestTableSinkFactory} is registered.
 */
public class TableSinkFactoryServiceTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testValidProperties() {
        Map<String, String> props = properties();
        assertTrue(
                TableFactoryService.find(TableSinkFactory.class, props)
                        instanceof TestTableSinkFactory);
    }

    @Test
    public void testInvalidContext() {
        thrown.expect(NoMatchingTableFactoryException.class);
        Map<String, String> props = properties();
        props.put(CONNECTOR_TYPE, "unknown-connector-type");
        TableFactoryService.find(TableSinkFactory.class, props);
    }

    @Test
    public void testDifferentContextVersion() {
        Map<String, String> props = properties();
        props.put(CONNECTOR_PROPERTY_VERSION, "2");
        // the table source should still be found
        assertTrue(
                TableFactoryService.find(TableSinkFactory.class, props)
                        instanceof TestTableSinkFactory);
    }

    @Test
    public void testUnsupportedProperty() {
        thrown.expect(NoMatchingTableFactoryException.class);
        thrown.expectMessage(
                "The matching candidates:\n"
                        + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                        + "Unsupported property keys:\n"
                        + "format.path_new");
        Map<String, String> props = properties();
        props.put("format.path_new", "/new/path");
        TableFactoryService.find(TableSinkFactory.class, props);
    }

    @Test
    public void testMissingProperty() {
        thrown.expect(NoMatchingTableFactoryException.class);
        thrown.expectMessage(
                "The matching candidates:\n"
                        + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                        + "Missing properties:\n"
                        + "format.type=test");
        Map<String, String> props = properties();
        props.remove(TableFactoryService.FORMAT_TYPE);
        TableFactoryService.find(TableSinkFactory.class, props);
    }

    @Test
    public void testMismatchedProperty() {
        thrown.expect(NoMatchingTableFactoryException.class);
        thrown.expectMessage(
                "The matching candidates:\n"
                        + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                        + "Mismatched properties:\n"
                        + "'format.type' expects 'test', but is 'test_new'");
        Map<String, String> props = properties();
        props.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST + "_new");
        TableFactoryService.find(TableSinkFactory.class, props);
    }

    @Test
    public void testMissingAndMismatchedProperty() {
        thrown.expect(NoMatchingTableFactoryException.class);
        thrown.expectMessage(
                "The matching candidates:\n"
                        + "org.apache.flink.table.factories.TestTableSinkFactory\n"
                        + "Missing properties:\n"
                        + "required.test=required-0\n"
                        + "Mismatched properties:\n"
                        + "'format.type' expects 'test', but is 'test_new'");
        Map<String, String> props = properties();
        props.put(TableFactoryService.FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST + "_new");
        props.remove(REQUIRED_TEST);
        TableFactoryService.find(TableSinkFactory.class, props);
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
