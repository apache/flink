/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.output;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests that snapshot utils can properly snapshot an operator. */
public class SnapshotUtilsTest {

    private static final List<String> EXPECTED_CALL_OPERATOR_SNAPSHOT =
            Arrays.asList("prepareSnapshotPreBarrier", "snapshotState", "notifyCheckpointComplete");

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final List<String> ACTUAL_ORDER_TRACKING =
            Collections.synchronizedList(new ArrayList<>(EXPECTED_CALL_OPERATOR_SNAPSHOT.size()));

    private static SnapshotType actualSnapshotType;

    @Test
    public void testSnapshotUtilsLifecycleWithDefaultSavepointFormatType() throws Exception {
        testSnapshotUtilsLifecycleWithSavepointFormatType(SavepointFormatType.DEFAULT);
    }

    @Test
    public void testSnapshotUtilsLifecycleWithCanonicalSavepointFormatType() throws Exception {
        testSnapshotUtilsLifecycleWithSavepointFormatType(SavepointFormatType.CANONICAL);
    }

    @Test
    public void testSnapshotUtilsLifecycleWithNativeSavepointFormatType() throws Exception {
        testSnapshotUtilsLifecycleWithSavepointFormatType(SavepointFormatType.NATIVE);
    }

    private void testSnapshotUtilsLifecycleWithSavepointFormatType(
            SavepointFormatType savepointFormatType) throws Exception {
        ACTUAL_ORDER_TRACKING.clear();
        StreamOperator<Void> operator = new LifecycleOperator();
        Path path = new Path(folder.newFolder().getAbsolutePath());

        SnapshotUtils.snapshot(
                operator, 0, 0L, true, false, new Configuration(), path, savepointFormatType);

        Assert.assertEquals(SavepointType.savepoint(savepointFormatType), actualSnapshotType);
        Assert.assertEquals(EXPECTED_CALL_OPERATOR_SNAPSHOT, ACTUAL_ORDER_TRACKING);
    }

    private static class LifecycleOperator implements StreamOperator<Void> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open() throws Exception {
            ACTUAL_ORDER_TRACKING.add("open");
        }

        @Override
        public void finish() throws Exception {
            ACTUAL_ORDER_TRACKING.add("finish");
        }

        @Override
        public void close() throws Exception {
            ACTUAL_ORDER_TRACKING.add("close");
        }

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
            ACTUAL_ORDER_TRACKING.add("prepareSnapshotPreBarrier");
        }

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            ACTUAL_ORDER_TRACKING.add("snapshotState");
            actualSnapshotType = checkpointOptions.getCheckpointType();
            return new OperatorSnapshotFutures();
        }

        @Override
        public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
                throws Exception {
            ACTUAL_ORDER_TRACKING.add("initializeState");
        }

        @Override
        public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
            ACTUAL_ORDER_TRACKING.add("setKeyContextElement1");
        }

        @Override
        public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
            ACTUAL_ORDER_TRACKING.add("setKeyContextElement2");
        }

        @Override
        public OperatorMetricGroup getMetricGroup() {
            ACTUAL_ORDER_TRACKING.add("getMetricGroup");
            return null;
        }

        @Override
        public OperatorID getOperatorID() {
            ACTUAL_ORDER_TRACKING.add("getOperatorID");
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            ACTUAL_ORDER_TRACKING.add("notifyCheckpointComplete");
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        @Override
        public void setCurrentKey(Object key) {
            ACTUAL_ORDER_TRACKING.add("setCurrentKey");
        }

        @Override
        public Object getCurrentKey() {
            ACTUAL_ORDER_TRACKING.add("getCurrentKey");
            return null;
        }
    }
}
