/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordWriterFactory {
	private static final Logger LOG = LoggerFactory.getLogger(RecordWriterFactory.class);

	public static <OUT extends IOReadableWritable> RecordWriter<OUT> createRecordWriter(ResultPartitionWriter bufferWriter, ChannelSelector<OUT> channelSelector, long bufferTimeout) {

		RecordWriter<OUT> output;

		if (bufferTimeout >= 0) {
			output = new StreamRecordWriter<OUT>(bufferWriter, channelSelector, bufferTimeout);

			if (LOG.isTraceEnabled()) {
				LOG.trace("StreamRecordWriter initiated with {} bufferTimeout.", bufferTimeout);
			}
		} else {
			output = new RecordWriter<OUT>(bufferWriter, channelSelector);

			if (LOG.isTraceEnabled()) {
				LOG.trace("RecordWriter initiated.");
			}
		}

		return output;

	}

}
