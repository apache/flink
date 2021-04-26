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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeInput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeOutput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.write;

/**
 * {@link ChannelStateWriter} implemented using
 * {@link CheckpointStreamFactory.CheckpointStateOutputStream CheckpointStateOutputStreams}. Internally, it has by default
 * <ul>
 * <li>one stream per checkpoint; having multiple streams would mean more files written and more connections opened
 * (and more latency on restore)</li>
 * <li>one thread; having multiple threads means more connections, couples with the implementation and increases complexity</li>
 * </ul>
 * Thread-safety: this class is thread-safe when used with a thread-safe {@link ChannelStateWriteRequestExecutor executor}
 * (e.g. default {@link ChannelStateWriteRequestExecutorImpl}.
 */
@Internal
@ThreadSafe
public class ChannelStateWriterImpl implements ChannelStateWriter {

	private static final Logger LOG = LoggerFactory.getLogger(ChannelStateWriterImpl.class);
	private static final int DEFAULT_MAX_CHECKPOINTS = 5; // currently, only single in-flight checkpoint is supported

	private final String taskName;
	private final ChannelStateWriteRequestExecutor executor;
	private final ConcurrentMap<Long, ChannelStateWriteResult> results;
	private final int maxCheckpoints;

	/**
	 * Creates a {@link ChannelStateWriterImpl} with {@link #DEFAULT_MAX_CHECKPOINTS} as {@link #maxCheckpoints}.
	 */
	public ChannelStateWriterImpl(String taskName, CheckpointStorageWorkerView streamFactoryResolver) {
		this(taskName, streamFactoryResolver, DEFAULT_MAX_CHECKPOINTS);
	}

	/**
	 * Creates a {@link ChannelStateWriterImpl} with {@link ChannelStateSerializerImpl default} {@link ChannelStateSerializer},
	 * and a {@link ChannelStateWriteRequestExecutorImpl}.
	 *  @param taskName
	 * @param streamFactoryResolver a factory to obtain output stream factory for a given checkpoint
	 * @param maxCheckpoints        maximum number of checkpoints to be written currently or finished but not taken yet.
	 */
	ChannelStateWriterImpl(String taskName, CheckpointStorageWorkerView streamFactoryResolver, int maxCheckpoints) {
		this(
			taskName,
			new ConcurrentHashMap<>(maxCheckpoints),
			new ChannelStateWriteRequestExecutorImpl(taskName, new ChannelStateWriteRequestDispatcherImpl(streamFactoryResolver, new ChannelStateSerializerImpl())),
			maxCheckpoints);
	}

	ChannelStateWriterImpl(
			String taskName,
			ConcurrentMap<Long, ChannelStateWriteResult> results,
			ChannelStateWriteRequestExecutor executor,
			int maxCheckpoints) {
		this.taskName = taskName;
		this.results = results;
		this.maxCheckpoints = maxCheckpoints;
		this.executor = executor;
	}

	@Override
	public void start(long checkpointId, CheckpointOptions checkpointOptions) {
		results.keySet().forEach(oldCheckpointId -> abort(oldCheckpointId, new Exception("Starting new checkpoint " + checkpointId)));
		LOG.debug("{} starting checkpoint {} ({})", taskName, checkpointId, checkpointOptions);
		ChannelStateWriteResult result = new ChannelStateWriteResult();
		ChannelStateWriteResult put = results.computeIfAbsent(checkpointId, id -> {
			Preconditions.checkState(results.size() < maxCheckpoints, "results.size() > maxCheckpoints", results.size(), maxCheckpoints);
			enqueue(new CheckpointStartRequest(checkpointId, result, checkpointOptions.getTargetLocation()), false);
			return result;
		});
		Preconditions.checkArgument(put == result, "result future already present for checkpoint id: " + checkpointId);
	}

	@Override
	public void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> iterator) {
		LOG.debug(
			"{} adding input data, checkpoint id: {}, channel: {}, startSeqNum: {}",
			taskName,
			checkpointId,
			info,
			startSeqNum);
		enqueue(write(checkpointId, info, iterator), false);
	}

	@Override
	public void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
		LOG.debug(
			"{} adding output data, checkpoint id: {}, channel: {}, startSeqNum: {}, num buffers: {}",
			taskName,
			checkpointId,
			info,
			startSeqNum,
			data == null ? 0 : data.length);
		enqueue(write(checkpointId, info, checkBufferType(data)), false);
	}

	@Override
	public void finishInput(long checkpointId) {
		LOG.debug("{} finishing input data, checkpoint id: {}", taskName, checkpointId);
		enqueue(completeInput(checkpointId), false);
	}

	@Override
	public void finishOutput(long checkpointId) {
		LOG.debug("{} finishing output data, checkpoint id: {}", taskName, checkpointId);
		enqueue(completeOutput(checkpointId), false);
	}

	@Override
	public void abort(long checkpointId, Throwable cause) {
		LOG.debug("{} aborting, checkpoint id: {}", taskName, checkpointId);
		enqueue(ChannelStateWriteRequest.abort(checkpointId, cause), true); // abort already started
		enqueue(ChannelStateWriteRequest.abort(checkpointId, cause), false); // abort enqueued but not started
		results.remove(checkpointId);
	}

	@Override
	public ChannelStateWriteResult getWriteResult(long checkpointId) {
		LOG.debug("{} requested write result, checkpoint id: {}", taskName, checkpointId);
		ChannelStateWriteResult result = results.get(checkpointId);
		Preconditions.checkArgument(result != null, "channel state write result not found for checkpoint id " + checkpointId);
		return result;
	}

	@Override
	public void stop(long checkpointId) {
		LOG.debug("{} stopping checkpoint id: {}", taskName, checkpointId);
		results.remove(checkpointId);
	}

	public void open() {
		executor.start();
	}

	@Override
	public void close() throws IOException {
		results.clear();
		executor.close();
	}

	private void enqueue(ChannelStateWriteRequest request, boolean atTheFront) {
		// state check and previous errors check are performed inside the worker
		try {
			if (atTheFront) {
				executor.submitPriority(request);
			} else {
				executor.submit(request);
			}
		} catch (Exception e) {
			RuntimeException wrapped = new RuntimeException("unable to send request to worker", e);
			try {
				request.cancel(e);
			} catch (Exception cancelException) {
				wrapped.addSuppressed(cancelException);
			}
			throw wrapped;
		}
	}

	private static Buffer[] checkBufferType(Buffer... data) {
		if (data == null) {
			return new Buffer[0];
		}
		try {
			for (Buffer buffer : data) {
				if (!buffer.isBuffer()) {
					throw new IllegalArgumentException(buildBufferTypeErrorMessage(buffer));
				}
			}
		} catch (Exception e) {
			for (Buffer buffer : data) {
				if (buffer.isBuffer()) {
					buffer.recycleBuffer();
				}
			}
			throw e;
		}
		return data;
	}

	private static String buildBufferTypeErrorMessage(Buffer buffer) {
		try {
			AbstractEvent event = EventSerializer.fromBuffer(buffer, ChannelStateWriterImpl.class.getClassLoader());
			return String.format("Should be buffer but [%s] found", event);
		}
		catch (Exception ex) {
			return "Should be buffer";
		}
	}
}
