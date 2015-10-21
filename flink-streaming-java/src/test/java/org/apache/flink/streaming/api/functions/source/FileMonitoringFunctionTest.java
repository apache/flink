/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction}.
 */
public class FileMonitoringFunctionTest {

	@Test
	public void testForEmptyLocation() throws Exception {
		final FileMonitoringFunction fileMonitoringFunction
			= new FileMonitoringFunction("?non-existing-path", 1L, FileMonitoringFunction.WatchType.ONLY_NEW_FILES);

        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fileMonitoringFunction.cancel();
            }
        }.start();

		fileMonitoringFunction.run(
            new StreamSource.NonWatermarkContext<Tuple3<String, Long, Long>>(
                new Object(),
                new Output<StreamRecord<Tuple3<String, Long, Long>>>() {
                    @Override
                    public void emitWatermark(Watermark mark) { }
                    @Override
                    public void collect(StreamRecord<Tuple3<String, Long, Long>> record) { }
                    @Override
                    public void close() { }
                })
        );
	}
}