/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * The default fetch task that fetches the records into the element queue.
 */
class FetchTask<E, SplitT extends SourceSplit> implements SplitFetcherTask {
	private final SplitReader<E, SplitT> splitReader;
	private final BlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
	private final Consumer<Collection<String>> splitFinishedCallback;
	private RecordsWithSplitIds<E> lastRecords;
	private Thread runningThread;
	private volatile boolean wakeup;

	FetchTask(
			SplitReader<E, SplitT> splitReader,
			BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			Consumer<Collection<String>> splitFinishedCallback,
			Thread runningThread) {
		this.splitReader = splitReader;
		this.elementsQueue = elementsQueue;
		this.splitFinishedCallback = splitFinishedCallback;
		this.lastRecords = null;
		this.runningThread = runningThread;
		this.wakeup = false;
	}

	@Override
	public boolean run() throws InterruptedException {
		if (lastRecords == null) {
			lastRecords = splitReader.fetch();
		}
		if (!wakeup) {
			elementsQueue.put(lastRecords);
			splitFinishedCallback.accept(lastRecords.finishedSplits());
		}
		synchronized (this) {
			wakeup = false;
			lastRecords = null;
		}
		// The return value of fetch task does not matter.
		return true;
	}

	@Override
	public void wakeUp() {
		synchronized (this) {
			wakeup = true;
			if (lastRecords == null) {
				splitReader.wakeUp();
			} else {
				runningThread.interrupt();
			}
		}
	}

	@Override
	public String toString() {
		return "FetchTask";
	}
}
