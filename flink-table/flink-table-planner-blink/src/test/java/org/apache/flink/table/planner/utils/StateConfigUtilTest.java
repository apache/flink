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

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.util.StateConfigUtil;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link StateConfigUtil}. */
public class StateConfigUtilTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testRocksDBWithHeapTimer() throws Exception {
        File tempDir = tempFolder.newFolder().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.setString("state.backend", "rocksdb");
        conf.setString("state.backend.rocksdb.timer-service.factory", "HEAP");
        conf.setString("state.checkpoints.dir", "file://" + tempDir.toString());
        assertIsStateImmutable(false, conf);
    }

    @Test
    public void testRocksDBWithDefaultTimer() throws Exception {
        File tempDir = tempFolder.newFolder().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.setString("state.backend", "rocksdb");
        conf.setString("state.checkpoints.dir", "file://" + tempDir.toString());
        assertIsStateImmutable(true, conf);
    }

    @Test
    public void testHeapState() throws Exception {
        Configuration conf = new Configuration();
        assertIsStateImmutable(false, conf);
    }

    private void assertIsStateImmutable(boolean result, Configuration conf) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.fromElements("a", "b", "c")
                .keyBy(s -> s)
                .transform(
                        "testing",
                        BasicTypeInfo.BOOLEAN_TYPE_INFO,
                        new TestingStateBackendOperator())
                .addSink(new VerifyingSink());
        env.execute();
        assertEquals(Arrays.asList(result, result, result), VerifyingSink.RESULT);
    }

    @After
    public void before() {
        VerifyingSink.RESULT.clear();
    }

    private static final class TestingStateBackendOperator extends AbstractStreamOperator<Boolean>
            implements OneInputStreamOperator<String, Boolean> {

        private static final long serialVersionUID = 1L;

        private transient Boolean result = null;

        @Override
        public void open() throws Exception {
            super.open();
            this.result = StateConfigUtil.isStateImmutableInStateBackend(getKeyedStateBackend());
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            if (result != null) {
                output.collect(new StreamRecord<>(result));
            }
        }
    }

    private static final class VerifyingSink implements SinkFunction<Boolean> {
        private static final long serialVersionUID = 1L;

        private static final List<Boolean> RESULT = new ArrayList<>();

        @Override
        public void invoke(Boolean value, Context context) throws Exception {
            synchronized (RESULT) {
                RESULT.add(value);
            }
        }
    }
}
