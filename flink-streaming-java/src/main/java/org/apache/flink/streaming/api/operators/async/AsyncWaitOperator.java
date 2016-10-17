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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Internal
public class AsyncWaitOperator<IN, OUT>
	extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>
{
	private final int DEFAULT_BUFFER_SIZE = 1000;

	private static final long serialVersionUID = 1L;

	/**
	 * {@link TypeSerializer} for inputs while making snapshots.
	 */
	private transient TypeSerializer<IN> inTypeSerializer;
	private transient DataOutputSerializer outputSerializer;

	/**
	 * input stream elements from the state
	 */
	private transient List<StreamElement> inputsFromState;

	private transient TimestampedCollector<OUT> collector;

	private transient AsyncCollectorBuffer<IN, OUT> buffer;

	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private AsyncDataStream.OutputMode mode;

	public AsyncWaitOperator(AsyncFunction<IN, OUT> asyncFunction) {
		super(asyncFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	public void setBufferSize(int size) {
		Preconditions.checkArgument(size > 0, "The number of concurrent async operation should be greater than 0.");
		bufferSize = size;
	}

	public void setMode(AsyncDataStream.OutputMode mode) {
		this.mode = mode;
	}

	public void init() {
		this.buffer = new AsyncCollectorBuffer<>(bufferSize, mode);
		this.collector = new TimestampedCollector<>(output);
		this.buffer.setOutput(collector, output);

		this.outputSerializer = new DataOutputSerializer(128);
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		this.inTypeSerializer = this.getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());

		init();
	}

	@Override
	public void open() throws Exception {
		super.open();

		// process stream elements from state
		if (this.inputsFromState != null) {
			for (StreamElement element : this.inputsFromState) {
				if (element.isRecord()) {
					processElement(element.<IN>asRecord());
				} else {
					processWatermark(element.asWatermark());
				}
			}
			this.inputsFromState = null;
		}

		buffer.startEmitterThread();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		AsyncCollector<IN, OUT> collector = buffer.add(element);
		userFunction.asyncInvoke(element.getValue(), collector);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		buffer.add(mark);
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		List<StreamElement> elements = buffer.getStreamElementsInBuffer();

		serializeStreamElements(elements, out);
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		this.inputsFromState = deserializeStreamElements(in);
	}

	@Override
	public void close() throws Exception {
		super.close();

		buffer.waitEmpty();
		buffer.stopEmitterThread();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		buffer.stopEmitterThread();
	}

	private void serializeStreamElements(List<StreamElement> input,
										FSDataOutputStream stream) throws IOException {
		stream.write(input.size());

		for (StreamElement element : input) {
			if (element.isRecord()) {
				stream.write(1);

				StreamRecord<IN> record = element.asRecord();

				outputSerializer.clear();
				inTypeSerializer.serialize(record.getValue(), outputSerializer);
				outputSerializer.writeLong(record.getTimestamp());
				outputSerializer.writeBoolean(record.hasTimestamp());

				stream.write(outputSerializer.getCopyOfBuffer());
			}
			else {
				stream.write(0);

				Watermark watermark = element.asWatermark();

				outputSerializer.clear();
				outputSerializer.writeLong(watermark.getTimestamp());

				stream.write(outputSerializer.getCopyOfBuffer());
			}
		}
	}

	private List<StreamElement> deserializeStreamElements(FSDataInputStream stream) throws IOException {
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(stream);

		int size = wrapper.read();

		List<StreamElement> ret = new ArrayList<>(size);
		for (int i = 0; i < size; ++i) {
			int flag = wrapper.read();

			if (flag == 1) {
				IN val = inTypeSerializer.deserialize(wrapper);
				long ts = wrapper.readLong();
				boolean hasTS = wrapper.readBoolean();

				StreamRecord<IN> record = new StreamRecord<>(val, ts);
				if (!hasTS) {
					record.eraseTimestamp();
				}

				ret.add(record);
			}
			else {
				long ts = wrapper.readLong();

				ret.add(new Watermark(ts));
			}
		}

		return ret;
	}
}
