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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.StagedTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/** A factory to create table to support staging for test purpose. */
public class TestSupportsStagingTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "test-staging";

    public static final List<String> jobStatusChangeProcess = new LinkedList<>();

    private static final ConfigOption<String> DATA_DIR =
            ConfigOptions.key("data-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The data id used to write the rows.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        String dataDir = helper.getOptions().get(DATA_DIR);
        return new SupportsStagingTableSink(dataDir);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(DATA_DIR);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    /** A sink that supports staging. */
    private static class SupportsStagingTableSink implements DynamicTableSink, SupportsStaging {

        private String dataDir;
        private TestStagedTable stagedTable;

        public SupportsStagingTableSink(String dataDir) {
            this(dataDir, null);
        }

        public SupportsStagingTableSink(String dataDir, TestStagedTable stagedTable) {
            this.dataDir = dataDir;
            this.stagedTable = stagedTable;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    if (stagedTable != null) {
                        return dataStream
                                .addSink(new StagedSinkFunction(dataDir))
                                .setParallelism(1);
                    } else {
                        // otherwise, do nothing
                        return dataStream.addSink(new DiscardingSink<>());
                    }
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new SupportsStagingTableSink(dataDir, stagedTable);
        }

        @Override
        public String asSummaryString() {
            return "SupportsStagingTableSink";
        }

        @Override
        public StagedTable applyStaging(StagingContext context) {
            jobStatusChangeProcess.clear();
            stagedTable = new TestStagedTable(dataDir);
            return stagedTable;
        }
    }

    /** The sink for delete existing data. */
    private static class StagedSinkFunction extends RichSinkFunction<RowData> {

        private String dataDir;

        public StagedSinkFunction(String dataDir) {
            this.dataDir = dataDir;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            File parentDir = new File(dataDir);
            if (parentDir.exists()) {
                parentDir.delete();
            }
            parentDir.mkdirs();
            // write hidden file first
            new File(dataDir, "_data").createNewFile();
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            FileUtils.writeFileUtf8(
                    new File(dataDir, "_data"), value.getInt(0) + "," + value.getString(1));
        }
    }

    /** A StagedTable for test. */
    private static class TestStagedTable implements StagedTable {

        private final String dataDir;

        public TestStagedTable(String dataDir) {
            this.dataDir = dataDir;
        }

        @Override
        public void begin() {
            jobStatusChangeProcess.add("begin");
        }

        @Override
        public void commit() {
            jobStatusChangeProcess.add("commit");
            // Change hidden file to official file
            new File(dataDir, "_data").renameTo(new File(dataDir, "data"));
        }

        @Override
        public void abort() {
            jobStatusChangeProcess.add("abort");
        }
    }
}
