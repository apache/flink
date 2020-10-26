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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;

interface RecoveredChannelStateHandler<Info, Context> extends AutoCloseable {
	class BufferWithContext<Context> {
		final ChannelStateByteBuffer buffer;
		final Context context;

		BufferWithContext(ChannelStateByteBuffer buffer, Context context) {
			this.buffer = buffer;
			this.context = context;
		}
	}

	BufferWithContext<Context> getBuffer(Info info) throws IOException, InterruptedException;

	void recover(Info info, Context context) throws IOException;
}

class InputChannelRecoveredStateHandler implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {
	private final InputGate[] inputGates;

	InputChannelRecoveredStateHandler(InputGate[] inputGates) {
		this.inputGates = inputGates;
	}

	@Override
	public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo) throws IOException, InterruptedException {
		RecoveredInputChannel channel = getChannel(channelInfo);
		Buffer buffer = channel.requestBufferBlocking();
		return new BufferWithContext<>(wrap(buffer), buffer);
	}

	@Override
	public void recover(InputChannelInfo channelInfo, Buffer buffer) {
		if (buffer.readableBytes() > 0) {
			getChannel(channelInfo).onRecoveredStateBuffer(buffer);
		} else {
			buffer.recycleBuffer();
		}
	}

	@Override
	public void close() throws IOException {
		// note that we need to finish all RecoveredInputChannels, not just those with state
		for (final InputGate inputGate : inputGates) {
			inputGate.finishReadRecoveredState();
		}
	}

	private RecoveredInputChannel getChannel(InputChannelInfo info) {
		return (RecoveredInputChannel) inputGates[info.getGateIdx()].getChannel(info.getInputChannelIdx());
	}
}

class ResultSubpartitionRecoveredStateHandler implements RecoveredChannelStateHandler<ResultSubpartitionInfo, Tuple2<BufferBuilder, BufferConsumer>> {

	private final ResultPartitionWriter[] writers;
	private final boolean notifyAndBlockOnCompletion;

	ResultSubpartitionRecoveredStateHandler(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion) {
		this.writers = writers;
		this.notifyAndBlockOnCompletion = notifyAndBlockOnCompletion;
	}

	@Override
	public BufferWithContext<Tuple2<BufferBuilder, BufferConsumer>> getBuffer(ResultSubpartitionInfo subpartitionInfo) throws IOException, InterruptedException {
		BufferBuilder bufferBuilder = getSubpartition(subpartitionInfo).requestBufferBuilderBlocking();
		return new BufferWithContext<>(wrap(bufferBuilder), Tuple2.of(bufferBuilder, bufferBuilder.createBufferConsumer()));
	}

	@Override
	public void recover(ResultSubpartitionInfo subpartitionInfo, Tuple2<BufferBuilder, BufferConsumer> bufferBuilderAndConsumer) throws IOException {
		bufferBuilderAndConsumer.f0.finish();
		if (bufferBuilderAndConsumer.f1.isDataAvailable()) {
			boolean added = getSubpartition(subpartitionInfo).add(bufferBuilderAndConsumer.f1, Integer.MIN_VALUE);
			if (!added) {
				throw new IOException("Buffer consumer couldn't be added to ResultSubpartition");
			}
		} else {
			bufferBuilderAndConsumer.f1.close();
		}
	}

	private CheckpointedResultSubpartition getSubpartition(ResultSubpartitionInfo subpartitionInfo) {
		ResultPartitionWriter writer = writers[subpartitionInfo.getPartitionIdx()];
		if (writer instanceof CheckpointedResultPartition) {
			return ((CheckpointedResultPartition) writer).getCheckpointedSubpartition(subpartitionInfo.getSubPartitionIdx());
		} else {
			throw new IllegalStateException(
				"Cannot restore state to a non-checkpointable partition type: " + writer);
		}
	}

	@Override
	public void close() throws IOException {
		for (ResultPartitionWriter writer : writers) {
			if (writer instanceof CheckpointedResultPartition) {
				((CheckpointedResultPartition) writer).finishReadRecoveredState(notifyAndBlockOnCompletion);
			}
		}
	}
}
