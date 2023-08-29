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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalogLock;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RequireCatalogLock;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Factory for testing {@link HiveCatalogLock}. */
public class TestLockTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "test-lock";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new TestLockTableSink(context.getObjectIdentifier());
    }

    private static class TestLockTableSink implements DynamicTableSink, RequireCatalogLock {

        private final ObjectIdentifier tableIdentifier;

        private CatalogLock.Factory lockFactory;

        private TestLockTableSink(ObjectIdentifier tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return new SinkFunctionProvider() {

                @Override
                public Optional<Integer> getParallelism() {
                    // 3 tasks acquire lock
                    return Optional.of(3);
                }

                @Override
                public SinkFunction<RowData> createSinkFunction() {
                    return new TestLockSink(tableIdentifier, lockFactory);
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            TestLockTableSink sink = new TestLockTableSink(tableIdentifier);
            sink.lockFactory = lockFactory;
            return sink;
        }

        @Override
        public String asSummaryString() {
            return "test-lock";
        }

        @Override
        public void setLockFactory(CatalogLock.Factory lockFactory) {
            this.lockFactory = lockFactory;
        }
    }

    private static class TestLockSink extends RichSinkFunction<RowData> {

        private static final AtomicReference<TestLockSink> REFERENCE = new AtomicReference<>();

        private final ObjectIdentifier tableIdentifier;
        private final CatalogLock.Factory lockFactory;

        private transient CatalogLock lock;

        private TestLockSink(ObjectIdentifier tableIdentifier, CatalogLock.Factory lockFactory) {
            this.tableIdentifier = tableIdentifier;
            this.lockFactory = lockFactory;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            this.lock = lockFactory.create();
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            lock.runWithLock(
                    tableIdentifier.getDatabaseName(),
                    tableIdentifier.getObjectName(),
                    () -> {
                        REFERENCE.set(TestLockSink.this);
                        Thread.sleep(5000);
                        assertThat(REFERENCE.get()).isSameAs(TestLockSink.this);
                        return null;
                    });
        }
    }
}
