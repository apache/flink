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
package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the operator that reads the {@link FileInputSplit FileInputSplits} received from
 * the preceding {@link FileSplitMonitoringFunction}. This operator will receive just the split descriptors
 * and then read and emit records. This may lead to increased backpressure. To avoid this, we have another
 * thread ({@link SplitReader}) actually reading the splits and emitting the elements, which is separate from
 * the thread forwarding the checkpoint barriers. The two threads sync on the {@link StreamTask#getCheckpointLock()}
 * so that the checkpoints reflect the current state.
 * */
public class FileSplitReadOperator<OUT, S extends Serializable> extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<FileInputSplit, OUT>, OutputTypeConfigurable<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(FileSplitReadOperator.class);

	private static final FileInputSplit EOF = new FileInputSplit(-1, null, -1, -1, null);

	private transient SplitReader<S, OUT> reader;
	private transient TimestampedCollector<OUT> collector;

	private Configuration configuration;
	private FileInputFormat<OUT> format;
	private TypeSerializer<OUT> serializer;

	private Tuple3<List<FileInputSplit>, FileInputSplit, S> readerState;

	public FileSplitReadOperator(FileInputFormat<OUT> format, Configuration configuration) {
		this.format = checkNotNull(format);
		this.configuration = checkNotNull(configuration);
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		this.serializer = outTypeInfo.createSerializer(executionConfig);
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (this.serializer == null) {
			throw new IllegalStateException("The serializer has not been set. " +
				"Probably the setOutputType() was not called and this should not have happened. " +
				"Please Report it.");
		}

		this.format.configure(configuration);
		this.collector = new TimestampedCollector<>(output);

		Object checkpointLock = getContainingTask().getCheckpointLock();

		this.reader = new SplitReader<>(format, serializer, collector, checkpointLock, readerState);
		this.reader.start();
		this.readerState = null;
	}

	@Override
	public void processElement(StreamRecord<FileInputSplit> element) throws Exception {
		reader.addSplit(element.getValue());
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

	@Override
	public void dispose() {
		super.dispose();

		// first try to cancel it properly and
		// give it some time until it finishes
		reader.cancel();
		try {
			reader.join(200);
		} catch (InterruptedException e) {
			// we can ignore this
		}

		// if the above did not work, then interrupt the thread repeatedly
		while (reader.isAlive()) {

			StringBuilder bld = new StringBuilder();
			StackTraceElement[] stack = reader.getStackTrace();
			for (StackTraceElement e : stack) {
				bld.append(e).append('\n');
			}
			LOG.warn("The reader is stuck in method:\n {}", bld.toString());

			reader.interrupt();
			try {
				reader.join(50);
			} catch (InterruptedException e) {
				// we can ignore this
			}
		}
		reader = null;
		collector = null;
		configuration = null;
		format = null;
		serializer = null;
	}

	@Override
	public void close() throws Exception {
		super.close();

		// signal that no more splits will come, wait for the reader to finish
		// and close the collector. Further cleaning up is handled by the dispose().

		if (reader != null && reader.isAlive() && reader.isRunning()) {
			// add a dummy element to signal that no more splits will
			// arrive and wait until the reader finishes
			reader.addSplit(EOF);
			reader.join();
		}
		collector.close();
	}

	private class SplitReader<S extends Serializable, OT> extends Thread {

		private volatile boolean isRunning;

		private final FileInputFormat<OT> format;
		private final TypeSerializer<OT> serializer;

		private final Object checkpointLock;
		private final TimestampedCollector<OT> collector;

		private final Object lock = new Object();

		private final Queue<FileInputSplit> pendingSplits;

		SplitReader(FileInputFormat<OT> format,
					TypeSerializer<OT> serializer,
					TimestampedCollector<OT> collector,
					Object checkpointLock,
					Tuple3<List<FileInputSplit>, FileInputSplit, S> restoredState) {

			this.format = checkNotNull(format, "Unspecified FileInputFormat.");
			this.serializer = checkNotNull(serializer, "Unspecified Serialized.");

			this.pendingSplits = new LinkedList<>();
			this.collector = collector;
			this.checkpointLock = checkpointLock;
			this.isRunning = true;

			// this is the case where a task recovers from a previous failed attempt
			if (restoredState != null) {
				List<FileInputSplit> pending = restoredState.f0;
				FileInputSplit current = restoredState.f1;
				S formatState = restoredState.f2;

				if (this.format instanceof CheckpointableInputFormat && current != null) {
					((CheckpointableInputFormat) format).restore(current, formatState);
				} else {
					LOG.warn("The format used is not checkpointable.");
				}

				if (current != null) {
					pendingSplits.add(current);
				}

				for (FileInputSplit split : pending) {
					pendingSplits.add(split);
				}
			}
		}

		void addSplit(FileInputSplit split) {
			Preconditions.checkNotNull(split);
			synchronized (lock) {
				this.pendingSplits.add(split);
			}
		}

		public boolean isRunning() {
			return this.isRunning;
		}

		@Override
		public void run() {
			FileInputSplit split = null;
			try {
				while (this.isRunning) {

					// get the next split to read.
					// locking is needed because checkpointing will
					// ask for a consistent snapshot of the list.
					synchronized (lock) {
						split = this.pendingSplits.peek();
					}

					if (split == null) {
						Thread.sleep(50);
						continue;
					}

					if (split.equals(EOF)) {
						isRunning = false;
						break;
					}

					synchronized (checkpointLock) {
						synchronized (lock) {
							split = this.pendingSplits.poll();
						}
						this.format.open(split);
					}

					try {
						OT nextElement = serializer.createInstance();
						do {
							synchronized (checkpointLock) {
								nextElement = format.nextRecord(nextElement);
								if (nextElement != null) {
									collector.collect(nextElement);
								}
							}
						} while (nextElement != null && !format.reachedEnd());
					} finally {
						this.format.close();
					}
				}
				LOG.info("Split Reader terminated, and exiting normally.");

			} catch (Throwable e) {
				if (isRunning) {
					LOG.error("Caught exception processing split: ", split);
				}
				getContainingTask().failExternally(e);
			}
		}

		Tuple3<List<FileInputSplit>, FileInputSplit, S> getReaderState() throws IOException {
			// take a consistent snapshot of the pending splits
			// and that of the format, which includes the split currently being read and
			// where we are in the split.

			List<FileInputSplit> snapshot;
			Tuple2<FileInputSplit, S> formatState = null;
			synchronized (lock) {
				snapshot = new ArrayList<>(this.pendingSplits.size());
				for (FileInputSplit split: this.pendingSplits) {
					snapshot.add(split);
				}

				if (this.format instanceof CheckpointableInputFormat) {
					formatState = ((CheckpointableInputFormat) format).getCurrentChannelState();
				} else {
					LOG.warn("The format used is not checkpointable.");
				}
			}

			return new Tuple3<>(snapshot, formatState.f0, formatState.f1);
		}

		public void cancel() {
			this.isRunning = false;
		}
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		final AbstractStateBackend.CheckpointStateOutputStream os =
			this.getStateBackend().createCheckpointStateOutputStream(checkpointId, timestamp);

		final ObjectOutputStream oos = new ObjectOutputStream(os);
		final AbstractStateBackend.CheckpointStateOutputView ov = new AbstractStateBackend.CheckpointStateOutputView(os);

		Tuple3<List<FileInputSplit>, FileInputSplit, S> readerState = this.reader.getReaderState();
		List<FileInputSplit> pendingSplits = readerState.f0;
		FileInputSplit currSplit = readerState.f1;
		S formatState = readerState.f2;

		// write the current split
		oos.writeObject(currSplit);

		// write the pending ones
		ov.writeInt(pendingSplits.size());
		for (FileInputSplit split : pendingSplits) {
			oos.writeObject(split);
		}

		// write the state of the reading channel
		oos.writeObject(formatState);

		taskState.setOperatorState(os.closeAndGetHandle());
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);

		StreamStateHandle stream = (StreamStateHandle)state.getOperatorState();

		final InputStream is = stream.getState(getUserCodeClassloader());
		final ObjectInputStream ois = new ObjectInputStream(is);
		final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(is);

		// read the split that was being read
		FileInputSplit currSplit = (FileInputSplit) ois.readObject();

		// read the pending splits list
		List<FileInputSplit> pendingSplits = new LinkedList<>();
		int noOfSplits = div.readInt();
		for (int i = 0; i < noOfSplits; i++) {
			FileInputSplit split = (FileInputSplit) ois.readObject();
			pendingSplits.add(split);
		}

		// read the state of the format
		S formatState = (S) ois.readObject();

		// set the whole reader state for the open() to find.
		this.readerState = new Tuple3<>(pendingSplits, currSplit, formatState);
		div.close();
	}
}
