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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
 *
 * <p>The thread is daemonic, because it is only a utility thread.
 */
public class OutputFlusher extends Thread {
	/** Default name for the output flush thread, if no name with a task reference is given. */
	@VisibleForTesting
	public static final String OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

	private static final Logger LOG = LoggerFactory.getLogger(OutputFlusher.class);

	private final RecordWriter<?> recordWriter;
	private final long timeout;
	private final MailboxExecutor mailboxExecutor;

	private volatile boolean running = true;

	public OutputFlusher(
			RecordWriter<?> recordWriter,
			String taskName,
			long timeout,
			MailboxExecutor mailboxExecutor) {
		super(OUTPUT_FLUSH_THREAD_NAME + " for " + taskName);
		Preconditions.checkArgument(timeout > 0);
		this.recordWriter = recordWriter;
		this.timeout = timeout;
		this.mailboxExecutor = mailboxExecutor;
	}

	public void terminate() {
		running = false;
		interrupt();
	}

	@Override
	public void run() {
		try {
			while (running) {
				try {
					Thread.sleep(timeout);
				} catch (InterruptedException e) {
					// propagate this if we are still running, because it should not happen
					// in that case
					if (running) {
						throw new Exception(e);
					}
				}

				recordWriter.flushAll();
			}
		} catch (Throwable t) {
			LOG.error("An exception happened while flushing the outputs", t);
			mailboxExecutor.execute(
				() -> {
					throw new FlinkException("OutputFlusher thread has failed", t);
				},
				"OutputFlusher");
		}
	}

	/**
	 * Closeable collection of {@link OutputFlusher}.
	 */
	public static class OutputFlushers implements AutoCloseable {
		private final ArrayList<OutputFlusher> outputFlushers = new ArrayList<>();
		private boolean isClosed;

		public void addOutputFlusher(OutputFlusher outputFlusher) {
			checkState(!isClosed);
			outputFlushers.add(outputFlusher);
		}

		public void close() {
			isClosed = true;
			for (OutputFlusher outputFlusher : outputFlushers) {
				outputFlusher.terminate();
			}
			for (OutputFlusher outputFlusher : outputFlushers) {
				try {
					outputFlusher.join();
				} catch (InterruptedException e) {
					// ignore on close
					// restore interrupt flag to fast exit further blocking calls
					Thread.currentThread().interrupt();
				}
			}
		}

		public boolean isEmpty() {
			return outputFlushers.isEmpty();
		}
	}
}
