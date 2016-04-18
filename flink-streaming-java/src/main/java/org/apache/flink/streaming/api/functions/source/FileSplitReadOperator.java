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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.AsynchronousException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the operator that reads the splits received from {@link FileSplitMonitoringFunction}.
 * This operator will receive just the split descriptors and then read and emit records. This may lead
 * to backpressure. To avoid this, we will have another thread actually reading the splits and
 * another forwarding the checkpoint barriers. The two should sync so that the checkpoints reflect the
 * current state.
 * */
public class FileSplitReadOperator<OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<FileInputSplit, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(FileSplitReadOperator.class);

	private transient SplitReader<OUT> reader;
	private transient TimestampedCollector<OUT> collector;

	/** This field is used to forward an exception that is caught in the reader thread. */
	private volatile AsynchronousException asyncException;

	private Configuration configuration;
	private FileInputFormat<OUT> format;
	private TypeInformation<OUT> typeInfo;

	public FileSplitReadOperator(FileInputFormat<OUT> format, TypeInformation<OUT> typeInfo, Configuration configuration) {
		this.format = format;
		this.typeInfo = typeInfo;
		this.configuration = configuration;

		// this is for the extra thread that is reading,
		// the tests were not terminating because the success
		// exception was caught by the extra thread, and never
		// forwarded.

		// for now, and to keep the extra thread to avoid backpressure,
		// we just disable chaining for the operator, so that it runs on
		// another thread. This will change later for a cleaner design.
		setChainingStrategy(ChainingStrategy.NEVER);
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.format.configure(configuration);
		this.collector = new TimestampedCollector<>(output);

		TypeSerializer<OUT> serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
		Object checkpointLock = getContainingTask().getCheckpointLock();

		this.reader = new SplitReader<>(this, format, serializer, collector, checkpointLock);
		this.reader.start();
	}

	@Override
	public void processElement(StreamRecord<FileInputSplit> element) throws Exception {
		checkReaderException();
		this.reader.addSplit(element.getValue());
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if there are watermarks, checking the async exception here
		// guarantees that it will eventually be thrown
		checkReaderException();
		output.emitWatermark(mark);
	}

	/**
	 * Check whether an exception was thrown in the split reader . This will rethrow that
	 * exception in case on occurred.
	 */
	private void checkReaderException() throws AsynchronousException {
		if (asyncException != null) {
			throw asyncException;
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.reader.cancel();
		this.reader.interrupt();
		this.collector.close();
	}

	private class SplitReader<OT> extends Thread {

		private final FileSplitReadOperator owner;

		private volatile boolean isRunning;

		private final Queue<FileInputSplit> pendingSplits;

		private final FileInputFormat<OT> format;
		private final TypeSerializer<OT> serializer;

		private final Object checkpointLock;
		private final TimestampedCollector<OT> collector;

		SplitReader(FileSplitReadOperator operator,
					FileInputFormat<OT> format,
					TypeSerializer<OT> serializer,
					TimestampedCollector<OT> collector,
					Object checkpointLock) {

			this.owner = checkNotNull(operator, "Unspecified reading operator.");
			this.format = checkNotNull(format, "Unspecified FileInputFormat.");
			this.serializer = checkNotNull(serializer, "Unspecified Serialized.");

			this.pendingSplits = new ConcurrentLinkedQueue<>();
			this.collector = collector;
			this.checkpointLock = checkpointLock;
			this.isRunning = true;
		}

		void addSplit(FileInputSplit split) {
			Preconditions.checkNotNull(split);
			LOG.info("Adding split to read queue: " + split);
			this.pendingSplits.add(split);
		}

		@Override
		public void run() {
			FileInputSplit split = null;

			try {
				while (this.isRunning) {

					// get the next split to read
					split = this.pendingSplits.poll();
					if (split == null) {
						Thread.sleep(50);
						continue;
					}

					boolean isOpen = false;
					try {
						this.format.open(split);
						isOpen = true;
						OT nextElement = serializer.createInstance();
						do {
							nextElement = format.nextRecord(nextElement);
							if (nextElement != null) {
								synchronized (checkpointLock) {
									collector.collect(nextElement);
									// checkpointing should be here.
								}
							}
						} while (nextElement != null && !format.reachedEnd());
					} finally {
						if (isOpen) {
							this.format.close();
						}
					}
				}

				LOG.info("Split Reader terminated, and exiting normally.");

			} catch (IOException e) {
				if (isRunning) {
					LOG.error("Caught exception while reading split: ", split);
				}
				if (owner.asyncException == null) {
					owner.asyncException = new AsynchronousException(e);
				}

			} catch (InterruptedException e) {
				LOG.error("Reader thread was interrupted: ", e.getMessage());
			}
		}

		public void cancel() {
			this.isRunning = false;
		}
	}
}
