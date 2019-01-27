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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class waits for {@code expectedRepetitionsOfExpectedRecord} number of occurrences of the
 * {@code expectedRecord}. {@code expectedRepetitionsOfExpectedRecord} is correlated with number of input channels.
 */
public class ReceiverThread<T extends IOReadableWritable> extends CheckedThread {
	private static final Logger LOG = LoggerFactory.getLogger(ReceiverThread.class);
	private final T value;
	private MutableRecordReader<T> reader;

	private CompletableFuture<Long> expectedRecord = new CompletableFuture<>();
	private CompletableFuture<?> recordsProcessed = new CompletableFuture<>();

	private volatile boolean running;

	ReceiverThread(T value) {
		setName(this.getClass().getName());

		this.running = true;
		this.value = checkNotNull(value);
	}

	public synchronized CompletableFuture<?> setExpectedRecord(long record) {
		checkState(!expectedRecord.isDone());
		checkState(!recordsProcessed.isDone());
		expectedRecord.complete(record);
		return recordsProcessed;
	}

	public void setupReader(SingleInputGate[] inputGates) {
		InputGate inputGate;
		if (inputGates.length > 1) {
			inputGate = new UnionInputGate(inputGates);
		} else {
			inputGate = inputGates[0];
		}
		reader = new MutableRecordReader<>(
			inputGate,
			new String[]{
				EnvironmentInformation.getTemporaryFileDirectory()
			},
			new Configuration());
	}

	private synchronized CompletableFuture<Long> getExpectedRecord() {
		return expectedRecord;
	}

	protected synchronized void finishProcessingExpectedRecords() throws IOException {
		checkState(expectedRecord.isDone());
		checkState(!recordsProcessed.isDone());

		recordsProcessed.complete(null);
		expectedRecord = new CompletableFuture<>();
		recordsProcessed = new CompletableFuture<>();
	}

	@Override
	public void go() throws Exception {
		try {
			while (running) {
				readRecords(getExpectedRecord().get());
				finishProcessingExpectedRecords();
			}
		}
		catch (InterruptedException e) {
			if (running) {
				throw e;
			}
		} catch (Exception e) {
			LOG.error("Error when reading records.", e);
		}
	}

	protected void readRecords(long numRecordsExpected) throws Exception {
		if (numRecordsExpected > 0) {
			LOG.debug("readRecords(numRecordsPerChannel = {})", numRecordsExpected);
			long numRecordsReceived = 0;
			while (running && reader.next(value)) {
				++numRecordsReceived;
				if (numRecordsReceived == numRecordsExpected) {
					break;
				}
			}
		}
	}

	public void shutdown() {
		running = false;
		interrupt();
		expectedRecord.complete(0L);
	}

}
