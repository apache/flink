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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.testutils.junit.SharedObjects;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Provides a flexible testing harness for table factories.
 *
 * <p>This testing harness allows writing custom sources and sinks which can be directly
 * instantiated from the test. This avoids having to implement a factory, and enables using the
 * {@link SharedObjects} rule to get direct access to the underlying source/sink from the test.
 *
 * <p>The underlying source/sink must extend from {@link SourceBase} or {@link SinkBase}.
 *
 * <p>Example:
 *
 * <pre>{@code
 * public class CustomSourceTest {
 *    {@literal @}Rule public SharedObjects sharedObjects = SharedObjects.create();
 *
 *    {@literal @}Test
 *    public void test() {
 *        SharedReference<List<Long>> appliedLimits = sharedObjects.add(new ArrayList<>());
 *        TableDescriptor sourceDescriptor =
 *                TableFactoryHarness.newBuilder()
 *                        .schema(Schema.derived())
 *                        .source(new CustomSource(appliedLimits))
 *                        .build();
 *        tEnv.createTable("T", sourceDescriptor);
 *
 *        tEnv.explainSql("SELECT * FROM T LIMIT 42");
 *
 *        assertEquals(1, appliedLimits.get().size());
 *        assertEquals((Long) 42L, appliedLimits.get().get(0));
 *    }
 *
 *    private static class CustomSource extends TableFactoryHarness.ScanSourceBase
 *            implements SupportsLimitPushDown {
 *        private final SharedReference<List<Long>> appliedLimits;
 *
 *        CustomSource(SharedReference<List<Long>> appliedLimits) {
 *            this.appliedLimits = appliedLimits;
 *        }
 *
 *        public void applyLimit(long limit) {
 *            appliedLimits.get().add(limit);
 *        }
 *    }
 * }
 * }</pre>
 */
public class TableFactoryHarness {

    /** Factory identifier for {@link Factory}. */
    public static final String IDENTIFIER = "harness";

    // ---------------------------------------------------------------------------------------------

    /**
     * Creates a builder for a new {@link TableDescriptor} specialized for this harness.
     *
     * <p>Use this method to create a {@link TableDescriptor} and passing in the source / sink
     * implementation you want to use. The descriptor can for example be used with {@link
     * TableEnvironment#createTable(String, TableDescriptor)} to register a table.
     */
    public static HarnessTableDescriptor.Builder newBuilder() {
        return new HarnessTableDescriptor.Builder();
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Factory which creates a source / sink defined in the specialized {@link HarnessCatalogTable}.
     */
    public static class Factory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

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
            return Collections.emptySet();
        }

        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            final FactoryUtil.TableFactoryHelper factoryHelper =
                    FactoryUtil.createTableFactoryHelper(this, context);
            factoryHelper.validate();

            final HarnessCatalogTable catalogTable =
                    (HarnessCatalogTable) context.getCatalogTable().getOrigin();
            if (catalogTable.source == null) {
                throw new ValidationException(
                        String.format(
                                "Table '%s' has no source configured.",
                                context.getObjectIdentifier()));
            }

            catalogTable.source.factoryContext = context;
            return catalogTable.source;
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            final FactoryUtil.TableFactoryHelper factoryHelper =
                    FactoryUtil.createTableFactoryHelper(this, context);
            factoryHelper.validate();

            final HarnessCatalogTable catalogTable =
                    (HarnessCatalogTable) context.getCatalogTable().getOrigin();
            if (catalogTable.sink == null) {
                throw new ValidationException(
                        String.format(
                                "Table '%s' has no sink configured.",
                                context.getObjectIdentifier()));
            }

            catalogTable.sink.factoryContext = context;
            return catalogTable.sink;
        }
    }

    /**
     * Specialized version of {@link TableDescriptor} which allows passing a custom source / sink to
     * the {@link CatalogTable} created from it.
     */
    private static class HarnessTableDescriptor extends TableDescriptor {

        private final @Nullable SourceBase source;
        private final @Nullable SinkBase sink;

        private HarnessTableDescriptor(
                Schema schema, @Nullable SourceBase source, @Nullable SinkBase sink) {
            super(
                    schema,
                    Collections.singletonMap(FactoryUtil.CONNECTOR.key(), IDENTIFIER),
                    Collections.emptyList(),
                    null);

            this.source = source;
            this.sink = sink;
        }

        @Override
        public CatalogTable toCatalogTable() {
            return new HarnessCatalogTable(super.toCatalogTable(), source, sink);
        }

        /** Builder for {@link HarnessTableDescriptor}. */
        public static class Builder {
            private @Nullable Schema schema;
            private @Nullable SourceBase source;
            private @Nullable SinkBase sink;

            /** Define the schema of the {@link TableDescriptor}. */
            public Builder schema(Schema schema) {
                this.schema = schema;
                return this;
            }

            /** Use a bounded {@link ScanTableSource} which produces no data. */
            public Builder boundedScanSource() {
                return source(new ScanSourceBase(true) {});
            }

            /** Use an unbounded {@link ScanTableSource} which produces no data. */
            public Builder unboundedScanSource() {
                return source(new ScanSourceBase(false) {});
            }

            /**
             * Use an unbounded {@link ScanTableSource} with the given {@param changelogMode} which
             * produces no data.
             */
            public Builder unboundedScanSource(ChangelogMode changelogMode) {
                return source(
                        new ScanSourceBase(false) {
                            @Override
                            public ChangelogMode getChangelogMode() {
                                return changelogMode;
                            }
                        });
            }

            /** Use a {@link LookupTableSource} which produces no data. */
            public Builder lookupSource() {
                return source(new LookupSourceBase() {});
            }

            /** Use a custom {@link DynamicTableSource}. */
            public Builder source(SourceBase source) {
                this.source = source;
                return this;
            }

            /** Use a {@link DynamicTableSink} which discards all data. */
            public Builder sink() {
                return sink(new SinkBase() {});
            }

            /** Use a custom {@link DynamicTableSink}. */
            public Builder sink(SinkBase sink) {
                this.sink = sink;
                return this;
            }

            /** Builds a {@link TableDescriptor}. */
            public TableDescriptor build() {
                return new HarnessTableDescriptor(schema, source, sink);
            }
        }
    }

    /** Specialized {@link CatalogTable} which contains a custom source / sink. */
    private static class HarnessCatalogTable extends DefaultCatalogTable {

        private final @Nullable SourceBase source;
        private final @Nullable SinkBase sink;

        public HarnessCatalogTable(
                CatalogTable parentTable, @Nullable SourceBase source, @Nullable SinkBase sink) {
            super(
                    parentTable.getUnresolvedSchema(),
                    parentTable.getComment(),
                    parentTable.getPartitionKeys(),
                    parentTable.getOptions());

            this.source = source;
            this.sink = sink;
        }

        @Override
        public CatalogBaseTable copy() {
            return copy(getOptions());
        }

        @Override
        public CatalogTable copy(Map<String, String> options) {
            final CatalogTable parentTable =
                    CatalogTable.of(
                            getUnresolvedSchema(), getComment(), getPartitionKeys(), options);
            return new HarnessCatalogTable(parentTable, source, sink);
        }
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Base class for custom sources which implement {@link ScanTableSource}.
     *
     * <p>Most interface methods are default-implemented for convenience, but can be overridden when
     * necessary. By default, a {@link ScanRuntimeProvider} is used which doesn't produce anything.
     *
     * <p>Sources derived from this base class will also be provided the {@link
     * DynamicTableFactory.Context} of the factory which gives access to e.g. the {@link
     * CatalogTable}.
     */
    public abstract static class ScanSourceBase extends SourceBase implements ScanTableSource {

        private final boolean bounded;

        public ScanSourceBase() {
            this(true);
        }

        public ScanSourceBase(boolean bounded) {
            this.bounded = bounded;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            return SourceFunctionProvider.of(
                    new SourceFunction<RowData>() {
                        @Override
                        public void run(SourceContext<RowData> ctx) {}

                        @Override
                        public void cancel() {}
                    },
                    bounded);
        }
    }

    /**
     * Base class for custom sources which implement {@link LookupTableSource}.
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
     * Base class for custom sinks.
     *
     * <p>Most interface methods are default-implemented for convenience, but can be overridden when
     * necessary. By default, a {@link SinkRuntimeProvider} is used which does nothing.
     *
     * <p>Sinks derived from this base class will also be provided the {@link
     * DynamicTableFactory.Context} of the factory which gives access to e.g. the {@link
     * CatalogTable}.
     */
    public abstract static class SinkBase implements DynamicTableSink {

        private DynamicTableFactory.Context factoryContext;

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.all();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkFunctionProvider.of(
                    new SinkFunction<RowData>() {
                        @Override
                        public void invoke(RowData value, Context context1) {}
                    });
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
    }

    /** Base class for {@link ScanSourceBase} and {{@link LookupSourceBase}}. */
    private abstract static class SourceBase implements DynamicTableSource {
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
    }
}
