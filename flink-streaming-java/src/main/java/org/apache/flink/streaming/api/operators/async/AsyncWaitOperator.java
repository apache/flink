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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

@Internal
public class AsyncWaitOperator<IN, OUT>
	extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>
{
	private final int DEFAULT_BUFFER_SIZE = 1000;

	private static final long serialVersionUID = 1L;

	private final static String STATE_NAME = "_async_wait_operator_state_";

	/**
	 * {@link TypeSerializer} for inputs while making snapshots.
	 */
	private transient StreamElementSerializer<IN> inStreamElementSerializer;

	/**
	 * input stream elements from the state
	 */
	private transient ListState<StreamElement> recoveredStreamElements;

	private transient TimestampedCollector<OUT> collector;

	private transient AsyncCollectorBuffer<IN, OUT> buffer;

	/**
	 * Checkpoint lock from {@link StreamTask#lock}
	 */
	private transient Object checkpointLock;

	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private AsyncDataStream.OutputMode mode;

	/**
	 * For test only. Normally this flag is true, indicating that the Emitter Thread
	 * in the buffer will work.
	 */
	private boolean emitFlag = true;

	/**
	 * Test serializer used in unit test
	 */
	private StreamElementSerializer<IN> inStreamElementSerializerForTest;


	public AsyncWaitOperator(AsyncFunction<IN, OUT> asyncFunction) {
		super(asyncFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	public void setBufferSize(int size) {
		Preconditions.checkArgument(size > 0, "The number of concurrent async operation should be greater than 0.");
		bufferSize = size;
	}

	public void setOutputMode(AsyncDataStream.OutputMode mode) {
		this.mode = mode;
	}

	@VisibleForTesting
	public void setEmitFlag(boolean emitFlag) {
		this.emitFlag = emitFlag;
	}

	@VisibleForTesting
	public void setInStreamElementSerializerForTest(StreamElementSerializer<IN> serializer) {
		this.inStreamElementSerializerForTest = serializer;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		if (this.inStreamElementSerializerForTest != null) {
			this.inStreamElementSerializer = this.inStreamElementSerializerForTest;
		}
		else {
			this.inStreamElementSerializer = new StreamElementSerializer(this.getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader()));
		}

		this.collector = new TimestampedCollector<>(output);

		this.checkpointLock = containingTask.getCheckpointLock();

		this.buffer = new AsyncCollectorBuffer<>(bufferSize, mode, output, collector, this.checkpointLock, this);

	}

	@Override
	public void open() throws Exception {
		super.open();

		// process stream elements from state
		if (this.recoveredStreamElements != null) {
			for (StreamElement element : this.recoveredStreamElements.get()) {
				if (element.isRecord()) {
					processElement(element.<IN>asRecord());
				}
				else if (element.isWatermark()) {
					processWatermark(element.asWatermark());
				}
				else if (element.isLatencyMarker()) {
					processLatencyMarker(element.asLatencyMarker());
				}
				else {
					throw new Exception("Unknown record type: "+element.getClass());
				}
			}
			this.recoveredStreamElements = null;
		}


		if (emitFlag) {
			buffer.startEmitterThread();
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		AsyncCollector<IN, OUT> collector = buffer.addStreamRecord(element);

		userFunction.asyncInvoke(element.getValue(), collector);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		buffer.addWatermark(mark);
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		buffer.addLatencyMarker(latencyMarker);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		ListState<StreamElement> partitionableState =
				getOperatorStateBackend().getOperatorState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
		partitionableState.clear();

		Collection<StreamElement> elements = buffer.getStreamElementsInBuffer();
		for (StreamElement element : elements) {
			partitionableState.add(element);
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		recoveredStreamElements =
				context.getManagedOperatorStateStore().getOperatorState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

	}

	@Override
	public void close() throws Exception {
		// for test only, we do not have to wait until buffer is empty.
		if (!emitFlag) {
			return;
		}

		buffer.waitEmpty();
		buffer.stopEmitterThread();

		super.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		buffer.stopEmitterThread();
	}

	public void sendLatencyMarker(LatencyMarker marker) throws Exception {
		super.processLatencyMarker(marker);
	}
}
