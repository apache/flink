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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * TODO write javadoc
 * <p>
 * - open a list state per snapshot process
 * - book-keep snapshot logs
 * - Clean up state when a savepoint is complete - ONLY in-transit records who do NOT belong in other snapshots
 *
 * @param <IN>
 */
@Internal
public class StreamIterationHead<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;

	private volatile RecordWriterOutput<IN>[] outputs;

	private UpstreamLogger<IN> upstreamLogger;

	private Object lock;

	@Override
	public void init() throws Exception {
		this.lock = getCheckpointLock();
		getConfiguration().setStreamOperator(new UpstreamLogger(getConfiguration()));
		operatorChain = new OperatorChain<>(this);
		this.upstreamLogger = (UpstreamLogger<IN>) operatorChain.getHeadOperator();
	}

	@Override
	protected void run() throws Exception {

		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}
		final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId,
			getEnvironment().getTaskInfo().getIndexOfThisSubtask());
		final long iterationWaitTime = getConfiguration().getIterationWaitTime();
		final boolean shouldWait = iterationWaitTime > 0;

		final BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel
			= new ArrayBlockingQueue<>(1);

		// offer the queue for the tail
		BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work 
		try {
			outputs = (RecordWriterOutput<IN>[]) getStreamOutputs();

			// If timestamps are enabled we make sure to remove cyclic watermark dependencies
			if (isSerializingTimestamps()) {
				for (RecordWriterOutput<IN> output : outputs) {
					output.emitWatermark(new Watermark(Long.MAX_VALUE));
				}
			}

			synchronized (lock) {
				//emit in-flight events in the upstream log upon recovery
				for (StreamRecord<IN> rec : upstreamLogger.getReplayLog()) {
					for (RecordWriterOutput<IN> output : outputs) {
						output.collect(rec);
					}
				}
				upstreamLogger.clearLog();
			}

			while (running) {
				Either<StreamRecord<IN>, CheckpointBarrier> nextRecord = shouldWait ?
					dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
					dataChannel.take();

				synchronized (lock) {

					if (nextRecord != null) {

						if (nextRecord.isLeft()) {
							//log in-transit records whose effects are not part of ongoing snapshots
							upstreamLogger.logRecord(nextRecord.left());

							//always forward records in transit 
							for (RecordWriterOutput<IN> output : outputs) {
								output.collect(nextRecord.left());
							}
						} else {
							//upon marker from tail (markers should loop back in FIFO order)
							checkpointState(new CheckpointMetaData(nextRecord.right().getId(), nextRecord.right().getTimestamp()), nextRecord.right().getCheckpointOptions(), new CheckpointMetrics());
							upstreamLogger.discardSlice();
						}

					} else {
						break;
					}
				}
			}
		} finally {
			// make sure that we remove the queue from the broker, to prevent a resource leak
			BlockingQueueBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}


	@Override
	protected void cancelTask() {
		running = false;
	}

	@Override
	protected void cleanup() throws Exception {
		//nothing to cleanup
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {

		//create a buffer for logging the initiated snapshot and broadcast a barrier for it
		synchronized (getCheckpointLock()) {
			upstreamLogger.createSlice(String.valueOf(checkpointMetaData.getCheckpointId()));
			operatorChain.broadcastCheckpointBarrier(checkpointMetaData.getCheckpointId(), checkpointMetaData.getTimestamp(), checkpointOptions);
		}

		return true;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the shared blocking
	 * queue for the back channel. The identification string is unique per parallel head/tail pair
	 * per iteration per job.
	 *
	 * @param jid          The job ID.
	 * @param iterationID  The id of the iteration in the job.
	 * @param subtaskIndex The parallel subtask number
	 * @return The identification string.
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}

	/**
	 * An internal operator that solely serves as a state logging facility for persisting,
	 * partitioning and restoring output logs for dataflow cycles consistently. To support concurrency,
	 * logs are being sliced proportionally to the number of concurrent snapshots. This allows committed
	 * output logs to be uniquely identified and cleared after each complete checkpoint.
	 * <p>
	 * The design is based on the following assumptions:
	 * <p>
	 * - A slice is named after a checkpoint ID. Checkpoint IDs are numerically ordered within an execution.
	 * - Each checkpoint barrier arrives back in FIFO order, thus we discard log slices in respective FIFO order.
	 * - Upon restoration the logger sorts sliced logs in the same FIFO order and returns an Iterable that
	 * gives a singular view of the log.
	 * <p>
	 * TODO it seems that ListState.clear does not unregister state. We need to put a hook for that.
	 *
	 * @param <IN>
	 */
	public static class UpstreamLogger<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {

		private final StreamConfig config;

		private LinkedList<ListState<StreamRecord<IN>>> slicedLog = new LinkedList<>();

		private UpstreamLogger(StreamConfig config) {
			this.config = config;
		}

		public void logRecord(StreamRecord<IN> record) throws Exception {
			if (!slicedLog.isEmpty()) {
				slicedLog.getLast().add(record);
			}
		}

		public void createSlice(String sliceID) throws Exception {
			ListState<StreamRecord<IN>> nextSlice =
				getOperatorStateBackend().getOperatorState(new ListStateDescriptor<>(sliceID,
					config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader())));
			slicedLog.addLast(nextSlice);
		}

		public void discardSlice() {
			ListState<StreamRecord<IN>> logToEvict = slicedLog.pollFirst();
			logToEvict.clear();
		}

		public Iterable<StreamRecord<IN>> getReplayLog() throws Exception {
			final List<String> logSlices = new ArrayList<>(getOperatorStateBackend().getRegisteredStateNames());
			Collections.sort(logSlices, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return Long.valueOf(o1).compareTo(Long.valueOf(o2));
				}
			});

			final List<Iterator<StreamRecord<IN>>> wrappedIterators = new ArrayList<>();
			for (String splitID : logSlices) {
				wrappedIterators.add(getOperatorStateBackend()
					.getOperatorState(new ListStateDescriptor<>(splitID,
						config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader()))).get().iterator());
			}

			if (wrappedIterators.size() == 0) {
				return new Iterable<StreamRecord<IN>>() {
					@Override
					public Iterator<StreamRecord<IN>> iterator() {
						return Collections.emptyListIterator();
					}
				};
			}

			return new Iterable<StreamRecord<IN>>() {
				@Override
				public Iterator<StreamRecord<IN>> iterator() {

					return new Iterator<StreamRecord<IN>>() {
						int indx = 0;
						Iterator<StreamRecord<IN>> currentIterator = wrappedIterators.get(0);

						@Override
						public boolean hasNext() {
							if (!currentIterator.hasNext()) {
								progressLog();
							}
							return currentIterator.hasNext();
						}

						@Override
						public StreamRecord<IN> next() {
							if (!currentIterator.hasNext() && indx < wrappedIterators.size()) {
								progressLog();
							}
							return currentIterator.next();
						}

						private void progressLog() {
							while (!currentIterator.hasNext() && ++indx < wrappedIterators.size()) {
								currentIterator = wrappedIterators.get(indx);
							}
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}

					};
				}
			};
		}

		public void clearLog() throws Exception {
			for (String outputLogs : getOperatorStateBackend().getRegisteredStateNames()) {
				getOperatorStateBackend().getOperatorState(new ListStateDescriptor<>(outputLogs,
					config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader()))).clear();
			}
		}


		@Override
		public void open() throws Exception {
			super.open();
		}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<IN>> output) {
			super.setup(containingTask, config, output);
		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
			//nothing to do
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			//nothing to do
		}

	}

}
