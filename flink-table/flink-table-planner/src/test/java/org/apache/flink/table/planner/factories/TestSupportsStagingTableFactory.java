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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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

    public static final List<String> JOB_STATUS_CHANGE_PROCESS = new LinkedList<>();
    public static final List<SupportsStaging.StagingPurpose> STAGING_PURPOSE_LIST =
            new LinkedList<>();

    private static final ConfigOption<String> DATA_DIR =
            ConfigOptions.key("data-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The data id used to write the rows.");

    private static final ConfigOption<Boolean> SINK_FAIL =
            ConfigOptions.key("sink-fail")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If set to true, then sink will throw an exception causing the job to fail, used to verify the TestStagedTable#abort.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        String dataDir = helper.getOptions().get(DATA_DIR);
        boolean sinkFail = helper.getOptions().get(SINK_FAIL);
        return new SupportsStagingTableSink(dataDir, sinkFail);
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
        return Collections.singleton(SINK_FAIL);
    }

    /** A sink that supports staging. */
    private static class SupportsStagingTableSink implements DynamicTableSink, SupportsStaging {

        private final String dataDir;
        private final boolean sinkFail;
        private TestStagedTable stagedTable;

        public SupportsStagingTableSink(String dataDir, boolean sinkFail) {
            this(dataDir, sinkFail, null);
        }

        public SupportsStagingTableSink(
                String dataDir, boolean sinkFail, TestStagedTable stagedTable) {
            this.dataDir = dataDir;
            this.sinkFail = sinkFail;
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
                    return dataStream
                            .addSink(new StagedSinkFunction(dataDir, sinkFail))
                            .setParallelism(1);
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new SupportsStagingTableSink(dataDir, sinkFail, stagedTable);
        }

        @Override
        public String asSummaryString() {
            return "SupportsStagingTableSink";
        }

        @Override
        public StagedTable applyStaging(StagingContext context) {
            JOB_STATUS_CHANGE_PROCESS.clear();
            STAGING_PURPOSE_LIST.clear();
            stagedTable = new TestStagedTable(dataDir);
            STAGING_PURPOSE_LIST.add(context.getStagingPurpose());
            return stagedTable;
        }
    }

    /** The sink for delete existing data. */
    private static class StagedSinkFunction extends RichSinkFunction<RowData> {

        private final String dataDir;
        private final boolean sinkFail;

        public StagedSinkFunction(String dataDir, boolean sinkFail) {
            this.dataDir = dataDir;
            this.sinkFail = sinkFail;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
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
            if (sinkFail) {
                throw new RuntimeException("Test StagedTable abort method.");
            }
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
            JOB_STATUS_CHANGE_PROCESS.add("begin");
        }

        @Override
        public void commit() {
            JOB_STATUS_CHANGE_PROCESS.add("commit");
            // Change hidden file to official file
            new File(dataDir, "_data").renameTo(new File(dataDir, "data"));
        }

        @Override
        public void abort() {
            JOB_STATUS_CHANGE_PROCESS.add("abort");
        }
    }
}
