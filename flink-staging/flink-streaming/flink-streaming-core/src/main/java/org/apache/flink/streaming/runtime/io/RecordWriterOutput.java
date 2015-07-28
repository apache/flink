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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Output} that sends data using a {@link RecordWriter}.
 */
public class RecordWriterOutput<OUT> implements Output<StreamRecord<OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriterOutput.class);

	private RecordWriter<SerializationDelegate<Object>> recordWriter;
	
	private SerializationDelegate<Object> serializationDelegate;

	@SuppressWarnings("unchecked")
	public RecordWriterOutput(
			RecordWriter<?> recordWriter,
			TypeSerializer<OUT> outSerializer,
			boolean enableWatermarkMultiplexing) {
		
		Preconditions.checkNotNull(recordWriter);

		this.recordWriter = (RecordWriter<SerializationDelegate<Object>>) recordWriter;

		TypeSerializer<Object> outRecordSerializer;
		if (enableWatermarkMultiplexing) {
			outRecordSerializer = new MultiplexingStreamRecordSerializer<OUT>(outSerializer);
		} else {
			outRecordSerializer = (TypeSerializer<Object>) (TypeSerializer<?>) new StreamRecordSerializer<OUT>(outSerializer);
		}

		if (outSerializer != null) {
			serializationDelegate = new SerializationDelegate<Object>(outRecordSerializer);
		}
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		serializationDelegate.setInstance(record);

		try {
			recordWriter.emit(serializationDelegate);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Emit failed: {}", e);
			}
			throw new RuntimeException("Element emission failed.", e);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		serializationDelegate.setInstance(mark);
		
		try {
			recordWriter.broadcastEmit(serializationDelegate);
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Watermark emit failed: {}", e);
			}
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			if (recordWriter instanceof StreamRecordWriter) {
				((StreamRecordWriter<?>) recordWriter).close();
			} else {
				recordWriter.flush();
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Failed to flush final output", e);
		}
	}

	public void clearBuffers() {
		recordWriter.clearBuffers();
	}

	public void broadcastEvent(TaskEvent barrier) throws IOException, InterruptedException {
		recordWriter.broadcastEvent(barrier);
	}
}
