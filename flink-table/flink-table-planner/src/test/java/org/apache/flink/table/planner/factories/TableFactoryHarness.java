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

package org.apache.flink.table.planner.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides a flexible testing harness for table factories.
 *
 * <p>This testing harness allows writing custom sources and sinks which can be directly
 * instantiated from the test. This avoids having to implement a factory, and enables using the
 * {@link SharedObjects} rule to get direct access to the underlying source/sink from the test.
 *
 * <p>Note that the underlying source/sink must be {@link Serializable}. It is recommended to extend
 * from {@link ScanSourceBase}, {@link LookupSourceBase}, or {@link SinkBase} which provide default
 * implementations for most methods as well as some convenience methods.
 *
 * <p>The harness provides a {@link Factory}. You can register a source / sink through configuration
 * by passing a base64-encoded serialization. The harness provides convenience methods to make this
 * process as simple as possible.
 *
 * <p>Example:
 *
 * <pre>{@code
 * public class CustomSourceTest {
 *     {@literal @}Rule public SharedObjects sharedObjects = SharedObjects.create();
 *
 *     {@literal @}Test
 *     public void test() {
 *         SharedReference<List<Long>> appliedLimits = sharedObjects.add(new ArrayList<>());
 *
 *         Schema schema = Schema.newBuilder().build();
 *         TableDescriptor sourceDescriptor = TableFactoryHarness.forSource(schema,
 *             new CustomSource(appliedLimits));
 *
 *         tEnv.createTable("T", sourceDescriptor);
 *         tEnv.explainSql("SELECT * FROM T LIMIT 42");
 *
 *         assertEquals(1, appliedLimits.get().size());
 *         assertEquals((Long) 42L, appliedLimits.get().get(0));
 *     }
 *
 *     private static class CustomSource extends ScanSourceBase implements SupportsLimitPushDown {
 *         private final SharedReference<List<Long>> appliedLimits;
 *
 *         CustomSource(SharedReference<List<Long>> appliedLimits) {
 *             this.appliedLimits = appliedLimits;
 *         }
 *
 *         {@literal @}Override
 *         public void applyLimit(long limit) {
 *             appliedLimits.get().add(limit);
 *         }
 *     }
 * }
 * }</pre>
 */
public class TableFactoryHarness {

    /** Factory identifier for {@link Factory}. */
    public static final String IDENTIFIER = "harness";

    public static final ConfigOption<String> SOURCE =
            ConfigOptions.key("source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Serialized instance of DynamicTableSource (Base64-encoded)");

    public static final ConfigOption<String> SINK =
            ConfigOptions.key("sink")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Serialized instance of DynamicTableSink (Base64-encoded)");

    // ---------------------------------------------------------------------------------------------

    /** Creates a {@link TableDescriptor} for the given {@param schema} and {@param source}. */
    public static TableDescriptor forSource(Schema schema, SourceBase source) {
        return TableDescriptor.forConnector(IDENTIFIER)
                .schema(schema)
                .option(SOURCE, source.serialize())
                .build();
    }

    /** Creates a {@link TableDescriptor} for the given {@param schema} and {@param sink}. */
    public static TableDescriptor forSink(Schema schema, SinkBase sink) {
        return TableDescriptor.forConnector(IDENTIFIER)
                .schema(schema)
                .option(SINK, sink.serialize())
                .build();
    }

    // ---------------------------------------------------------------------------------------------

    /** Harness factory for creating sources / sinks from base64-encoded serialized strings. */
    public static class Factory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

        @Override
        public String factoryIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return new HashSet<>();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            Set<ConfigOption<?>> options = new HashSet<>();
            options.add(SOURCE);
            options.add(SINK);
            return options;
        }

        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            final FactoryUtil.TableFactoryHelper factoryHelper =
                    FactoryUtil.createTableFactoryHelper(this, context);
            factoryHelper.validate();

            final DynamicTableSource source =
                    deserializeSourceSink(factoryHelper.getOptions(), SOURCE);
            if (source instanceof SourceBase) {
                ((SourceBase) source).factoryContext = context;
            }

            return source;
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            final FactoryUtil.TableFactoryHelper factoryHelper =
                    FactoryUtil.createTableFactoryHelper(this, context);
            factoryHelper.validate();

            final DynamicTableSink sink = deserializeSourceSink(factoryHelper.getOptions(), SINK);
            if (sink instanceof SinkBase) {
                ((SinkBase) sink).factoryContext = context;
            }

            return sink;
        }

        private <T> T deserializeSourceSink(
                ReadableConfig options, ConfigOption<String> configOption) {
            final String serializedValue =
                    options.getOptional(configOption)
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    String.format(
                                                            "Missing option '%s'.",
                                                            configOption.key())));

            try {
                return InstantiationUtil.deserializeObject(
                        Base64.getDecoder().decode(serializedValue),
                        Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException | IOException e) {
                throw new ValidationException(
                        "Serialized source/sink could not be deserialized.", e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Serializes a source / sink into a base64-encoded string which can be used by {@link Factory}.
     *
     * <p>If your source / sink extends from {@link ScanSourceBase}, {@link LookupSourceBase}, or
     * {@link SinkBase}, you can use {@link SourceBase#serialize()} / {@link SinkBase#serialize()}
     * instead, or use {@link #forSource(Schema, SourceBase)} / {@link #forSink(Schema, SinkBase)}.
     */
    public static String serializeImplementation(Object obj) {
        try {
            return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(obj));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /** Creates a {@link ScanRuntimeProvider} which produces nothing. */
    public static ScanRuntimeProvider createEmptyScanProvider() {
        return SourceFunctionProvider.of(
                new SourceFunction<RowData>() {
                    @Override
                    public void run(SourceContext<RowData> ctx) {}

                    @Override
                    public void cancel() {}
                },
                true);
    }

    /** Creates a {@link SinkRuntimeProvider} which discards all records. */
    public static SinkRuntimeProvider createEmptySinkProvider() {
        return SinkFunctionProvider.of(
                new SinkFunction<RowData>() {
                    @Override
                    public void invoke(RowData value, Context context) {}
                });
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Serializable base class for custom sources which implement {@link ScanTableSource}.
     *
     * <p>Most interface methods are default-implemented for convenience, but can be overridden when
     * necessary. By default, a {@link ScanRuntimeProvider} is used which doesn't produce anything.
     *
     * <p>Sources derived from this base class will also be provided the {@link
     * DynamicTableFactory.Context} of the factory which gives access to e.g. the {@link
     * CatalogTable}.
     */
    public abstract static class ScanSourceBase extends SourceBase implements ScanTableSource {
        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            return createEmptyScanProvider();
        }
    }

    /**
     * Serializable base class for custom sources which implement {@link LookupTableSource}.
     *
     * <p>Most interface methods are default-implemented for convenience, but can be overridden when
     * necessary. By default, a {@link LookupRuntimeProvider} is used which doesn't produce
     * anything.
     *
     * <p>Sources derived from this base class will also be provided the {@link
     * DynamicTableFactory.Context} of the factory which gives access to e.g. the {@link
     * CatalogTable}.
     */
    public abstract static class LookupSourceBase extends SourceBase implements LookupTableSource {
        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
            return TableFunctionProvider.of(new TableFunction<RowData>() {});
        }
    }

    /**
     * Serializable base class for custom sinks.
     *
     * <p>Most interface methods are default-implemented for convenience, but can be overridden when
     * necessary. By default, a {@link SinkRuntimeProvider} is used which does nothing.
     *
     * <p>Sinks derived from this base class will also be provided the {@link
     * DynamicTableFactory.Context} of the factory which gives access to e.g. the {@link
     * CatalogTable}.
     */
    public abstract static class SinkBase implements Serializable, DynamicTableSink {

        private DynamicTableFactory.Context factoryContext;

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.all();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return createEmptySinkProvider();
        }

        @Override
        public DynamicTableSink copy() {
            return this;
        }

        @Override
        public String asSummaryString() {
            return "Unspecified Testing Sink";
        }

        public DynamicTableFactory.Context getFactoryContext() {
            return factoryContext;
        }

        public String serialize() {
            return serializeImplementation(this);
        }
    }

    /** Base class for {@link ScanSourceBase} and {{@link LookupSourceBase}}. */
    private abstract static class SourceBase implements Serializable, DynamicTableSource {
        private DynamicTableFactory.Context factoryContext;

        @Override
        public DynamicTableSource copy() {
            return this;
        }

        @Override
        public String asSummaryString() {
            return "Unspecified Testing Source";
        }

        public DynamicTableFactory.Context getFactoryContext() {
            return factoryContext;
        }

        public String serialize() {
            return serializeImplementation(this);
        }
    }
}
