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
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.api.functions.async.buffer.AsyncCollectorBuffer;
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

import java.util.Iterator;

/**
 * The {@link AsyncWaitOperator} will accept input {@link StreamElement} from previous operators,
 * pass them into {@link AsyncFunction}, make a snapshot for the inputs in the {@link AsyncCollectorBuffer}
 * while checkpointing, and restore the {@link AsyncCollectorBuffer} from previous state.
 * <p>
 * Note that due to newly added working thread, named {@link AsyncCollectorBuffer.Emitter},
 * if {@link AsyncWaitOperator} is chained with other operators, {@link StreamTask} has to make sure that
 * the the order to open operators in the operator chain should be from the tail operator to the head operator,
 * and order to close operators in the operator chain should be from the head operator to the tail operator.
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncWaitOperator<IN, OUT>
	extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
	implements OneInputStreamOperator<IN, OUT>
{
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

	private final int bufferSize;
	private final AsyncDataStream.OutputMode mode;


	public AsyncWaitOperator(AsyncFunction<IN, OUT> asyncFunction, int bufferSize, AsyncDataStream.OutputMode mode) {
		super(asyncFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;

		Preconditions.checkArgument(bufferSize > 0, "The number of concurrent async operation should be greater than 0.");
		this.bufferSize = bufferSize;

		this.mode = mode;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		this.inStreamElementSerializer =
				new StreamElementSerializer(this.getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));

		this.collector = new TimestampedCollector<>(output);

		this.checkpointLock = containingTask.getCheckpointLock();

		this.buffer = new AsyncCollectorBuffer<>(bufferSize, mode, output, collector, this.checkpointLock, this);
	}

	@Override
	public void open() throws Exception {
		super.open();

		// process stream elements from state, since the Emit thread will start soon as all elements from
		// previous state are in the AsyncCollectorBuffer, we have to make sure that the order to open all
		// operators in the operator chain should be from the tail operator to the head operator.
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

		buffer.startEmitterThread();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		AsyncCollector<OUT> collector = buffer.addStreamRecord(element);

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

		Iterator<StreamElement> iterator = buffer.getStreamElementsInBuffer();
		while (iterator.hasNext()) {
			partitionableState.add(iterator.next());
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		recoveredStreamElements =
				context.getOperatorStateStore().getOperatorState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

	}

	@Override
	public void close() throws Exception {
		try {
			buffer.waitEmpty();
		}
		finally {
			// make sure Emitter thread exits and close user function
			buffer.stopEmitterThread();

			super.close();
		}
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		buffer.stopEmitterThread();
	}

	public void sendLatencyMarker(LatencyMarker marker) throws Exception {
		super.processLatencyMarker(marker);
	}

	@VisibleForTesting
	public AsyncCollectorBuffer<IN, OUT> getBuffer() {
		return buffer;
	}
}
