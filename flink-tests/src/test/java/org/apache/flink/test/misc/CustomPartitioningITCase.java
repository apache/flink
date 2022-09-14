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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.test.util.JavaProgramTestBase;

import org.junit.Assert;

/** Integration tests for custom {@link Partitioner}. */
@SuppressWarnings("serial")
public class CustomPartitioningITCase extends JavaProgramTestBase {

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (!isCollectionExecution()) {
            Assert.assertTrue(env.getParallelism() > 1);
        }

        env.generateSequence(1, 1000)
                .partitionCustom(new AllZeroPartitioner(), new IdKeySelector<Long>())
                .map(new FailExceptInPartitionZeroMapper())
                .output(new DiscardingOutputFormat<Long>());

        env.execute();
    }

    private static class FailExceptInPartitionZeroMapper extends RichMapFunction<Long, Long> {

        @Override
        public Long map(Long value) throws Exception {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                return value;
            } else {
                throw new Exception("Received data in a partition other than partition 0");
            }
        }
    }

    private static class AllZeroPartitioner implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return 0;
        }
    }

    private static class IdKeySelector<T> implements KeySelector<T, T> {
        @Override
        public T getKey(T value) {
            return value;
        }
    }
}
