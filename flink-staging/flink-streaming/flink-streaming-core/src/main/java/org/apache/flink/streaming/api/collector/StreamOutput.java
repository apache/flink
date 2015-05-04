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

package org.apache.flink.streaming.api.collector;

import java.io.IOException;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamOutput<OUT> implements Collector<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamOutput.class);

	private RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;
	private SerializationDelegate<StreamRecord<OUT>> serializationDelegate;
	private StreamRecord<OUT> streamRecord;

	public StreamOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output, SerializationDelegate<StreamRecord<OUT>> serializationDelegate) {

		this.serializationDelegate = serializationDelegate;

		if (serializationDelegate != null) {
			this.streamRecord = serializationDelegate.getInstance();
		} else {
			throw new RuntimeException("Serializer cannot be null");
		}
		this.output = output;
	}

	public RecordWriter<SerializationDelegate<StreamRecord<OUT>>> getRecordWriter() {
		return output;
	}

	@Override
	public void collect(OUT record) {
		streamRecord.setObject(record);
		serializationDelegate.setInstance(streamRecord);

		try {
			output.emit(serializationDelegate);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Emit failed due to: {}", StringUtils.stringifyException(e));
			}
		}
	}

	@Override
	public void close() {
		if (output instanceof StreamRecordWriter) {
			((StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>) output).close();
		} else {
			try {
				output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void clearBuffers() {
		output.clearBuffers();
	}

	public void broadcastEvent(TaskEvent barrier) throws IOException, InterruptedException {
		output.broadcastEvent(barrier);
	}
}
