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

package org.apache.flink.connector.file.sink;

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.connector.file.sink.utils.PartSizeAndCheckpointRollingPolicy;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests for {@link FileSink}. */
public class FileSinkTest {

    @Test
    public void testCreateFileWriterWithTimerRegistered() throws IOException {
        TestSinkInitContext ctx = new TestSinkInitContext();
        FileSink<Integer> sink =
                FileSink.forRowFormat(
                                new Path("mock"), new IntegerFileSinkTestDataUtils.IntEncoder())
                        .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy<>(1024, true))
                        .build();
        sink.createWriter(ctx);
        assertEquals(ctx.getTestProcessingTimeService().getNumActiveTimers(), 1);
    }
}
