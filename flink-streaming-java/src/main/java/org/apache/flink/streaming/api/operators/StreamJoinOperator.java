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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.HeapWindowBuffer;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


public class StreamJoinOperator<K, IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, JoinFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 8650694601687319011L;
	private static final Logger LOG = LoggerFactory.getLogger(StreamJoinOperator.class);

	private HeapWindowBuffer<IN1> stream1Buffer;
	private HeapWindowBuffer<IN2> stream2Buffer;
	private final KeySelector<IN1, K> keySelector1;
	private final KeySelector<IN2, K> keySelector2;
	private long stream1WindowLength;
	private long stream2WindowLength;

	protected transient long currentWatermark1 = -1L;
	protected transient long currentWatermark2 = -1L;
	protected transient long currentWatermark = -1L;

	private TypeSerializer<IN1> inputSerializer1;
	private TypeSerializer<IN2> inputSerializer2;
	/**
	 * If this is true. The current processing time is set as the timestamp of incoming elements.
	 * This for use with a {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor}
	 * if eviction should happen based on processing time.
	 */
	private boolean setProcessingTime = false;

	public StreamJoinOperator(JoinFunction<IN1, IN2, OUT> userFunction,
					KeySelector<IN1, K> keySelector1,
					KeySelector<IN2, K> keySelector2,
					long stream1WindowLength,
					long stream2WindowLength,
					TypeSerializer<IN1> inputSerializer1,
					TypeSerializer<IN2> inputSerializer2) {
		super(userFunction);
		this.keySelector1 = requireNonNull(keySelector1);
		this.keySelector2 = requireNonNull(keySelector2);

		this.stream1WindowLength = requireNonNull(stream1WindowLength);
		this.stream2WindowLength = requireNonNull(stream2WindowLength);

		this.inputSerializer1 = requireNonNull(inputSerializer1);
		this.inputSerializer2 = requireNonNull(inputSerializer2);
	}

	@Override
	public void open() throws Exception {
		super.open();
		if (null == inputSerializer1 || null == inputSerializer2) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		this.stream1Buffer = new HeapWindowBuffer.Factory<IN1>().create();
		this.stream2Buffer = new HeapWindowBuffer.Factory<IN2>().create();
	}

	/**
	 * @param element record of stream1
	 * @throws Exception
	 */
	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}
		stream1Buffer.storeElement(element);

		if (setProcessingTime) {
			IN1 item1 = element.getValue();
			long time1 = element.getTimestamp();

			int expiredDataNum = 0;
			for (StreamRecord<IN2> record2 : stream2Buffer.getElements()) {
				IN2 item2 = record2.getValue();
				long time2 = record2.getTimestamp();
				if (time2 < time1 && time2 + this.stream2WindowLength >= time1) {
					if (keySelector1.getKey(item1).equals(keySelector2.getKey(item2))) {
						output.collect(new StreamRecord<>(userFunction.join(item1, item2)));
					}
				} else {
					expiredDataNum++;
				}
			}
			// clean data
			stream2Buffer.removeElements(expiredDataNum);
		}
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}
		stream2Buffer.storeElement(element);

		if (setProcessingTime) {
			IN2 item2 = element.getValue();
			long time2 = element.getTimestamp();

			int expiredDataNum = 0;
			for (StreamRecord<IN1> record1 : stream1Buffer.getElements()) {
				IN1 item1 = record1.getValue();
				long time1 = record1.getTimestamp();
				if (time1 <= time2 && time1 + this.stream1WindowLength >= time2) {
					if (keySelector1.getKey(item1).equals(keySelector2.getKey(item2))) {
						output.collect(new StreamRecord<>(userFunction.join(item1, item2)));
					}
				} else {
					expiredDataNum++;
				}
			}
			// clean data
			stream1Buffer.removeElements(expiredDataNum);
		}
	}

	/**
	 * Process join operator on element during [currentWaterMark, watermark)
	 * @param watermark
	 * @throws Exception
	 */
	private void processWatermark(long watermark) throws Exception{
		System.out.println("Watermark:" + String.valueOf(watermark));

		if(setProcessingTime) {
			return;
		}
		// process elements after current watermark1 and lower than mark
		for (StreamRecord<IN1> record1 : stream1Buffer.getElements()) {
			if(record1.getTimestamp() >= this.currentWatermark
					&& record1.getTimestamp() < watermark){
				for (StreamRecord<IN2> record2 : stream2Buffer.getElements()) {
					if(keySelector1.getKey(record1.getValue()).equals(keySelector2.getKey(record2.getValue()))) {
						if (record1.getTimestamp() >= record2.getTimestamp()
								&& record2.getTimestamp() + this.stream2WindowLength >= record1.getTimestamp()) {
							output.collect(new StreamRecord<>(userFunction.join(record1.getValue(), record2.getValue())));
						}
					}
				}
			}
		}

		for (StreamRecord<IN2> record2 : stream2Buffer.getElements()) {
			if(record2.getTimestamp() >= this.currentWatermark
					&& record2.getTimestamp() < watermark){
				for (StreamRecord<IN1> record1 : stream1Buffer.getElements()) {
					if(keySelector1.getKey(record1.getValue()).equals(keySelector2.getKey(record2.getValue()))) {
						if (record2.getTimestamp() > record1.getTimestamp()
								&& record1.getTimestamp() + this.stream1WindowLength >= record2.getTimestamp()) {
							output.collect(new StreamRecord<>(userFunction.join(record1.getValue(), record2.getValue())));
						}
					}
				}
			}
		}

		// clean data
		int stream1Expired = 0;
		for (StreamRecord<IN1> record1 : stream1Buffer.getElements()) {
			if (record1.getTimestamp() + this.stream1WindowLength < watermark) {
				stream1Expired++;
			} else {
				break;
			}
		}
		stream1Buffer.removeElements(stream1Expired);

		int stream2Expired = 0;
		for (StreamRecord<IN2> record2 : stream2Buffer.getElements()) {
			if (record2.getTimestamp() + this.stream2WindowLength < watermark) {
				stream2Expired++;
			} else {
				break;
			}
		}
		stream2Buffer.removeElements(stream2Expired);
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		long watermark = Math.min(mark.getTimestamp(), currentWatermark2);
		// process elements [currentWatermark, watermark)
		processWatermark(watermark);

		output.emitWatermark(mark);
		this.currentWatermark = watermark;
		this.currentWatermark1 = mark.getTimestamp();
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		long watermark = Math.min(mark.getTimestamp(), currentWatermark1);
		// process elements [currentWatermark, watermark)
		processWatermark(watermark);

		output.emitWatermark(mark);
		this.currentWatermark = watermark;
		this.currentWatermark2 = mark.getTimestamp();
	}

	/**
	 * When this flag is enabled the current processing time is set as the timestamp of elements
	 * upon arrival. This must be used, for example, when using the
	 * {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor} with processing
	 * time semantics.
	 */
	public StreamJoinOperator<K, IN1, IN2, OUT> enableSetProcessingTime(boolean setProcessingTime) {
		this.setProcessingTime = setProcessingTime;
		return this;
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		// we write the panes with the key/value maps into the stream
		StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		out.writeLong(stream1WindowLength);
		out.writeLong(stream2WindowLength);

		MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
		out.writeInt(stream1Buffer.size());
		for (StreamRecord<IN1> element: stream1Buffer.getElements()) {
			recordSerializer1.serialize(element, out);
		}

		MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
		out.writeInt(stream2Buffer.size());
		for (StreamRecord<IN2> element: stream2Buffer.getElements()) {
			recordSerializer2.serialize(element, out);
		}

		taskState.setOperatorState(out.closeAndGetHandle());
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		super.restoreState(taskState, recoveryTimestamp);

		final ClassLoader userClassloader = getUserCodeClassloader();

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(userClassloader);

		stream1WindowLength = in.readLong();
		stream2WindowLength = in.readLong();

		int numElements = in.readInt();

		MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
		for (int i = 0; i < numElements; i++) {
			stream1Buffer.storeElement(recordSerializer1.deserialize(in).<IN1>asRecord());
		}

		int numElements2 = in.readInt();
		MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
		for (int i = 0; i < numElements2; i++) {
			stream2Buffer.storeElement(recordSerializer2.deserialize(in).<IN2>asRecord());
		}
	}

}
