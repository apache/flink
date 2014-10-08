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

package org.apache.flink.streaming.io;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> implements ReaderBase {

	/**
	 * Readers for the two input types
	 */
	private MutableRecordReader<T1> reader1;
	private MutableRecordReader<T2> reader2;

	private boolean finishedReader1 = false;

	private boolean finishedReader2 = false;

	private boolean endOfSuperstepReader1 = false;

	private boolean endOfSuperstepReader2 = false;

	public CoRecordReader(MutableRecordReader<T1> reader1, MutableRecordReader<T2> reader2) {
		this.reader1 = reader1;
		this.reader2 = reader2;
	}

	@SuppressWarnings("unchecked")
	protected int getNextRecord(T1 target1, T2 target2) throws IOException, InterruptedException {
		do {
			if (finishedReader1 && finishedReader2) {
				return 0;
			}

			if (endOfSuperstepReader1 && endOfSuperstepReader2) {
				endOfSuperstepReader1 = false;
				endOfSuperstepReader2 = false;

				return 0;
			}

			if (!finishedReader1 && !endOfSuperstepReader1) {
				if (reader1.next(target1)) {
					return 1;
				}
				else if (reader1.isFinished()) {
					finishedReader1 = true;
				}
				else if (reader1.hasReachedEndOfSuperstep()) {
					endOfSuperstepReader1 = true;
				}
				else {
					throw new IOException("Unexpected return value from reader.");
				}
			}

			if (!finishedReader2 && !endOfSuperstepReader2) {
				if (reader2.next(target2)) {
					return 2;
				}
				else if (reader2.isFinished()) {
					finishedReader2 = true;
				}
				else if (reader2.hasReachedEndOfSuperstep()) {
					endOfSuperstepReader2 = true;
				}
				else {
					throw new IOException("Unexpected return value from reader.");
				}
			}
		} while (true);
	}

	@Override
	public boolean isFinished() {
		return reader1.isFinished() && reader2.isFinished();
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		reader1.subscribeToTaskEvent(eventListener, eventType);
		reader2.subscribeToTaskEvent(eventListener, eventType);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		reader1.sendTaskEvent(event);
		reader2.sendTaskEvent(event);
	}

	@Override
	public void setIterativeReader() {
		reader1.setIterativeReader();
		reader2.setIterativeReader();
	}

	@Override
	public void startNextSuperstep() {
		reader1.startNextSuperstep();
		reader2.startNextSuperstep();
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return reader1.hasReachedEndOfSuperstep() && reader2.hasReachedEndOfSuperstep();
	}
}
