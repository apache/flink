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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSinkMock;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSourceMock;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.factories.utils.FactoryMocks;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link FactoryUtil}. */
public class FactoryUtilTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMissingConnector() {
        expectError(
                "Table options do not contain an option key 'connector' for discovering a connector.");
        testError(options -> options.remove("connector"));
    }

    @Test
    public void testInvalidConnector() {
        expectError(
                "Could not find any factory for identifier 'FAIL' that implements '"
                        + DynamicTableFactory.class.getName()
                        + "' in the classpath.\n\n"
                        + "Available factory identifiers are:\n\n"
                        + "conflicting\nsink-only\nsource-only\ntest\ntest-connector");
        testError(options -> options.put("connector", "FAIL"));
    }

    @Test
    public void testConflictingConnector() {
        expectError(
                "Multiple factories for identifier 'conflicting' that implement '"
                        + DynamicTableFactory.class.getName()
                        + "' found in the classpath.\n"
                        + "\n"
                        + "Ambiguous factory classes are:\n"
                        + "\n"
                        + TestConflictingDynamicTableFactory1.class.getName()
                        + "\n"
                        + TestConflictingDynamicTableFactory2.class.getName());
        testError(
                options ->
                        options.put("connector", TestConflictingDynamicTableFactory1.IDENTIFIER));
    }

    @Test
    public void testMissingConnectorOption() {
        expectError(
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "target");
        testError(options -> options.remove("target"));
    }

    @Test
    public void testInvalidConnectorOption() {
        expectError("Invalid value for option 'buffer-size'.");
        testError(options -> options.put("buffer-size", "FAIL"));
    }

    @Test
    public void testMissingFormat() {
        expectError("Could not find required scan format 'value.format'.");
        testError(options -> options.remove("value.format"));
    }

    @Test
    public void testInvalidFormat() {
        expectError(
                "Could not find any factory for identifier 'FAIL' that implements '"
                        + DeserializationFormatFactory.class.getName()
                        + "' in the classpath.\n\n"
                        + "Available factory identifiers are:\n\n"
                        + "test-format");
        testError(options -> options.put("value.format", "FAIL"));
    }

    @Test
    public void testMissingFormatOption() {
        expectError("Error creating scan format 'test-format' in option space 'key.test-format.'.");
        expectError(
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "delimiter");
        testError(options -> options.remove("key.test-format.delimiter"));
    }

    @Test
    public void testInvalidFormatOption() {
        expectError("Invalid value for option 'fail-on-missing'.");
        testError(options -> options.put("key.test-format.fail-on-missing", "FAIL"));
    }

    @Test
    public void testSecretOption() {
        expectError(
                "Table options are:\n"
                        + "\n"
                        + "'buffer-size'='1000'\n"
                        + "'connector'='test-connector'\n"
                        + "'key.format'='test-format'\n"
                        + "'key.test-format.delimiter'=','\n"
                        + "'password'='******'\n"
                        + "'property-version'='1'\n"
                        + "'value.format'='test-format'\n"
                        + "'value.test-format.delimiter'='|'\n"
                        + "'value.test-format.fail-on-missing'='true'");
        testError(
                options -> {
                    options.remove("target");
                    options.put("password", "123");
                });
    }

    @Test
    public void testUnconsumedOption() {
        expectError(
                "Unsupported options found for 'test-connector'.\n\n"
                        + "Unsupported options:\n\n"
                        + "this-is-also-not-consumed\n"
                        + "this-is-not-consumed\n\n"
                        + "Supported options:\n\n"
                        + "buffer-size\n"
                        + "connector\n"
                        + "deprecated-target (deprecated)\n"
                        + "fallback-buffer-size\n"
                        + "format\n"
                        + "key.format\n"
                        + "key.test-format.changelog-mode\n"
                        + "key.test-format.delimiter\n"
                        + "key.test-format.deprecated-delimiter (deprecated)\n"
                        + "key.test-format.fail-on-missing\n"
                        + "key.test-format.fallback-fail-on-missing\n"
                        + "key.test-format.readable-metadata\n"
                        + "password\n"
                        + "property-version\n"
                        + "target\n"
                        + "value.format\n"
                        + "value.test-format.changelog-mode\n"
                        + "value.test-format.delimiter\n"
                        + "value.test-format.deprecated-delimiter (deprecated)\n"
                        + "value.test-format.fail-on-missing\n"
                        + "value.test-format.fallback-fail-on-missing\n"
                        + "value.test-format.readable-metadata");
        testError(
                options -> {
                    options.put("this-is-not-consumed", "42");
                    options.put("this-is-also-not-consumed", "true");
                });
    }

    @Test
    public void testAllOptions() {
        final Map<String, String> options = createAllOptions();
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock("|", true));
        assertEquals(expectedSource, actualSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock("|"));
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testDiscoveryForSeparateSourceSinkFactory() {
        final Map<String, String> options = createAllOptions();
        // the "test" source and sink factory is not in one factory class
        // see TestDynamicTableSinkFactory and TestDynamicTableSourceFactory
        options.put("connector", "test");

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock("|", true));
        assertEquals(expectedSource, actualSource);

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock("|"));
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testOptionalFormat() {
        final Map<String, String> options = createAllOptions();
        options.remove("key.format");
        options.remove("key.test-format.delimiter");
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock("MyTarget", null, new DecodingFormatMock("|", true));
        assertEquals(expectedSource, actualSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock("MyTarget", 1000L, null, new EncodingFormatMock("|"));
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testAlternativeValueFormat() {
        final Map<String, String> options = createAllOptions();
        options.remove("value.format");
        options.remove("value.test-format.delimiter");
        options.remove("value.test-format.fail-on-missing");
        options.put("format", "test-format");
        options.put("test-format.delimiter", ";");
        options.put("test-format.fail-on-missing", "true");
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock(";", true));
        assertEquals(expectedSource, actualSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock(";"));
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testConnectorErrorHint() {
        try {
            createTableSource(SCHEMA, Collections.singletonMap("connector", "sink-only"));
            fail();
        } catch (Exception e) {
            String errorMsg =
                    "Connector 'sink-only' can only be used as a sink. It cannot be used as a source.";
            assertThat(e, containsCause(new ValidationException(errorMsg)));
        }

        try {
            createTableSink(SCHEMA, Collections.singletonMap("connector", "source-only"));
            fail();
        } catch (Exception e) {
            String errorMsg =
                    "Connector 'source-only' can only be used as a source. It cannot be used as a sink.";
            assertThat(e, containsCause(new ValidationException(errorMsg)));
        }
    }

    @Test
    public void testRequiredPlaceholderOption() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(ConfigOptions.key("fields.#.min").intType().noDefaultValue());
        requiredOptions.add(
                ConfigOptions.key("no.placeholder.anymore")
                        .intType()
                        .noDefaultValue()
                        .withFallbackKeys("old.fields.#.min"));

        FactoryUtil.validateFactoryOptions(requiredOptions, new HashSet<>(), new Configuration());
    }

    @Test
    public void testCreateCatalog() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), TestCatalogFactory.IDENTIFIER);
        options.put(TestCatalogFactory.DEFAULT_DATABASE.key(), "my-database");

        final Catalog catalog =
                FactoryUtil.createCatalog(
                        "my-catalog",
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());
        assertTrue(catalog instanceof TestCatalogFactory.TestCatalog);

        final TestCatalogFactory.TestCatalog testCatalog = (TestCatalogFactory.TestCatalog) catalog;
        assertEquals(testCatalog.getName(), "my-catalog");
        assertEquals(
                testCatalog.getOptions().get(TestCatalogFactory.DEFAULT_DATABASE.key()),
                "my-database");
    }

    @Test
    public void testCatalogFactoryHelper() {
        final FactoryUtil.CatalogFactoryHelper helper1 =
                FactoryUtil.createCatalogFactoryHelper(
                        new TestCatalogFactory(),
                        new FactoryUtil.DefaultCatalogContext(
                                "test",
                                Collections.emptyMap(),
                                null,
                                Thread.currentThread().getContextClassLoader()));

        // No error
        helper1.validate();

        final FactoryUtil.CatalogFactoryHelper helper2 =
                FactoryUtil.createCatalogFactoryHelper(
                        new TestCatalogFactory(),
                        new FactoryUtil.DefaultCatalogContext(
                                "test",
                                Collections.singletonMap("x", "y"),
                                null,
                                Thread.currentThread().getContextClassLoader()));

        expectError("Unsupported options found for 'test-catalog'");
        helper2.validate();
    }

    @Test
    public void testFactoryHelperWithDeprecatedOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("deprecated-target", "MyTarget");
        options.put("fallback-buffer-size", "1000");
        options.put("value.format", "test-format");
        options.put("value.test-format.deprecated-delimiter", "|");
        options.put("value.test-format.fallback-fail-on-missing", "true");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestDynamicTableFactory(),
                        FactoryMocks.createTableContext(SCHEMA, options));
        helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, TestDynamicTableFactory.VALUE_FORMAT);
        helper.validate();
    }

    @Test
    public void testFactoryHelperWithMapOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("properties.prop-1", "value-1");
        options.put("properties.prop-2", "value-2");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestFactoryWithMap(), FactoryMocks.createTableContext(SCHEMA, options));

        helper.validate();
    }

    @Test
    public void testInvalidFactoryHelperWithMapOption() {
        expectError(
                "Unsupported options found for 'test-factory-with-map'.\n\n"
                        + "Unsupported options:\n\n"
                        + "unknown\n\n"
                        + "Supported options:\n\n"
                        + "connector\n"
                        + "properties\n"
                        + "properties.prop-1\n"
                        + "properties.prop-2\n"
                        + "property-version");

        final Map<String, String> options = new HashMap<>();
        options.put("properties.prop-1", "value-1");
        options.put("properties.prop-2", "value-2");
        options.put("unknown", "value-3");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestFactoryWithMap(), FactoryMocks.createTableContext(SCHEMA, options));
        helper.validate();
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private void expectError(String message) {
        thrown.expect(ValidationException.class);
        thrown.expect(containsMessage(message));
    }

    private static void testError(Consumer<Map<String, String>> optionModifier) {
        final Map<String, String> options = createAllOptions();
        optionModifier.accept(options);
        createTableSource(SCHEMA, options);
    }

    private static Map<String, String> createAllOptions() {
        final Map<String, String> options = new HashMap<>();
        // we use strings here to test realistic parsing
        options.put("property-version", "1");
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");
        options.put("key.format", "test-format");
        options.put("key.test-format.delimiter", ",");
        options.put("value.format", "test-format");
        options.put("value.test-format.delimiter", "|");
        options.put("value.test-format.fail-on-missing", "true");
        return options;
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class TestFactoryWithMap implements DynamicTableFactory {

        public static final ConfigOption<Map<String, String>> PROPERTIES =
                ConfigOptions.key("properties").mapType().noDefaultValue();

        @Override
        public String factoryIdentifier() {
            return "test-factory-with-map";
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.singleton(PROPERTIES);
        }
    }
}
