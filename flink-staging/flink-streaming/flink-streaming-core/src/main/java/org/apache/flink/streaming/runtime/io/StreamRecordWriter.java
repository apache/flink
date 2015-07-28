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

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This record writer keeps data in buffers at most for a certain timeout. It spawns a separate thread
 * that flushes the outputs in a defined interval, to make sure data does not linger in the buffers for too long.
 * 
 * @param <T> The type of elements written.
 */
public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	/** Default name for teh output flush thread, if no name with a task reference is given */
	private static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";
	
	
	/** The thread that periodically flushes the output, to give an upper latency bound */
	private final OutputFlusher outputFlusher;
	
	/** Flag indicating whether the output should be flushed after every element */
	private final boolean flushAlways;

	/** The exception encountered in the flushing thread */
	private Throwable flusherException;
	
	
	
	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, long timeout) {
		this(writer, channelSelector, timeout, null);
	}
	
	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector,
								long timeout, String taskName) {
		
		super(writer, channelSelector);
		
		checkArgument(timeout >= 0);
		
		if (timeout == 0) {
			flushAlways = true;
			outputFlusher = null;
		}
		else {
			flushAlways = false;

			String threadName = taskName == null ?
								DEFAULT_OUTPUT_FLUSH_THREAD_NAME : "Output Timeout Flusher - " + taskName;
			
			outputFlusher = new OutputFlusher(threadName, timeout);
			outputFlusher.start();
		}
	}
	
	@Override
	public void emit(T record) throws IOException, InterruptedException {
		checkErroneous();
		super.emit(record);
		if (flushAlways) {
			flush();
		}
	}

	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();
		super.broadcastEmit(record);
		if (flushAlways) {
			flush();
		}
	}

	/**
	 * Closes the writer. This stops the flushing thread (if there is one) and flushes all pending outputs.
	 * 
	 * @throws IOException I/O errors may happen during the final flush of the buffers.
	 */
	public void close() throws IOException {
		// propagate exceptions
		flush();
		
		if (outputFlusher != null) {
			try {
				outputFlusher.terminate();
				outputFlusher.join();
			}
			catch (InterruptedException e) {
				// ignore on close
			}
		}

		// final check for asynchronous errors, before we exit with a green light
		checkErroneous();
	}

	/**
	 * Notifies the writer that teh output flusher thread encountered an exception.
	 * 
	 * @param t The exception to report.
	 */
	void notifyFlusherException(Throwable t) {
		if (this.flusherException == null) {
			this.flusherException = t;
		}
	}
	
	private void checkErroneous() throws IOException {
		if (flusherException != null) {
			throw new IOException("An exception happened while flushing the outputs", flusherException);
		}
	}

	// ------------------------------------------------------------------------
	
	/**
	 * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
	 * 
	 * The thread is daemonic, because it is only a utility thread.
	 */
	private class OutputFlusher extends Thread {
		
		private final long timeout;
		
		private volatile boolean running = true;

		
		OutputFlusher(String name, long timeout) {
			super(name);
			setDaemon(true);
			this.timeout = timeout;
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
					}
					catch (InterruptedException e) {
						// propagate this if we are still running, because it should not happen
						// in that case
						if (running) {
							throw new Exception(e);
						}
					}
					
					// any errors here should let the thread come to a halt and be
					// recognized by the writer 
					flush();
				}
			}
			catch (Throwable t) {
				notifyFlusherException(t);
			}
		}
	}
}
