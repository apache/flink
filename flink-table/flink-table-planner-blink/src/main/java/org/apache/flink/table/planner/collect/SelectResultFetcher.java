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

package org.apache.flink.table.planner.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestGateway;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A fetcher which fetches query results from sink and provides exactly-once semantics.
 */
public class SelectResultFetcher {

	private static final String INIT_VERSION = "";
	private static final long MEMORY_SIZE = 64 * MemoryManager.DEFAULT_PAGE_SIZE * 3;
	private static final int DEFAULT_RETRY_MILLIS = 100;
	private static final long DEFAULT_ACCUMULATOR_GET_MILLIS = 10000;

	private static final Logger LOG = LoggerFactory.getLogger(SelectResultFetcher.class);

	private final CompletableFuture<OperatorID> operatorIdFuture;
	protected final TypeSerializer<Row> serializer;
	private final String accumulatorName;
	private final int retryMillis;

	private final BinaryRowData reuseRow;
	private final BinaryWriter reuseWriter;

	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final int pageNum;

	// TODO we currently don't have an external buffer which can store any type,
	//  so we have to temporarily use ResettableExternalBuffer,
	//  refactoring ResettableExternalBuffer instead of squeezing into it might be better
	private ResettableExternalBuffer uncheckpointedBuffer;
	private ResettableExternalBuffer checkpointedBuffer;
	private ResettableExternalBuffer.BufferIterator checkpointedIterator;
	private ResettableExternalBuffer standByBuffer;

	protected JobClient jobClient;

	private String version;
	private long offset;
	private long lastCheckpointedOffset;

	private boolean terminated;
	private boolean closed;

	public SelectResultFetcher(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<Row> serializer,
			String accumulatorName) {
		this(
			operatorIdFuture,
			serializer,
			accumulatorName,
			DEFAULT_RETRY_MILLIS);
	}

	@VisibleForTesting
	public SelectResultFetcher(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<Row> serializer,
			String accumulatorName,
			int retryMillis) {
		this.operatorIdFuture = operatorIdFuture;
		this.serializer = serializer;
		this.accumulatorName = accumulatorName;
		this.retryMillis = retryMillis;

		this.reuseRow = new BinaryRowData(1);
		this.reuseWriter = new BinaryRowWriter(reuseRow);

		Map<MemoryType, Long> memoryPools = new EnumMap<>(MemoryType.class);
		memoryPools.put(MemoryType.HEAP, MEMORY_SIZE);
		this.memoryManager = new MemoryManager(memoryPools, MemoryManager.DEFAULT_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.pageNum = (int) (MEMORY_SIZE / 3 / memoryManager.getPageSize());

		this.uncheckpointedBuffer = newBuffer();
		this.checkpointedBuffer = newBuffer();
		this.standByBuffer = newBuffer();

		this.version = INIT_VERSION;
		this.offset = 0;
		this.lastCheckpointedOffset = 0;

		this.terminated = false;
	}

	public void setJobClient(JobClient jobClient) {
		Preconditions.checkArgument(
			jobClient instanceof CoordinationRequestGateway,
			"Job client must be a CoordinationRequestGateway. This is a bug.");
		this.jobClient = jobClient;
	}

	@SuppressWarnings("unchecked")
	public List<Row> nextBatch() {
		if (closed) {
			return Collections.emptyList();
		}

		List<Row> checkpointedResults = getNextCheckpointedResult();
		if (checkpointedResults.size() > 0) {
			// we still have checkpointed results, just use them
			return checkpointedResults;
		} else if (terminated) {
			// no results, but job has terminated, we have to return
			return checkpointedResults;
		}

		// we're going to fetch some more
		while (true) {
			if (isJobTerminated()) {
				// job terminated, read results from accumulator
				// and move all results from uncheckpointed buffer to checkpointed buffer
				terminated = true;
				Tuple2<Long, CollectCoordinationResponse> accResults = getAccumulatorResults();
				dealWithResponse(accResults.f1, accResults.f0);
				checkpointFinalResults();
			} else {
				// job still running, try to fetch some results
				CollectCoordinationResponse<Row> response;
				try {
					response = sendRequest(version, offset);
				} catch (Exception e) {
					LOG.warn("An exception occurs when fetching query results", e);
					sleepBeforeRetry();
					continue;
				}
				dealWithResponse(response, offset);
			}

			// try to return results after fetching
			checkpointedResults = getNextCheckpointedResult();
			if (checkpointedResults.size() > 0) {
				// ok, we have results this time
				return checkpointedResults;
			} else if (terminated) {
				// still no results, but job has terminated, we have to return
				return checkpointedResults;
			} else {
				// still no results, but job is still running, retry
				sleepBeforeRetry();
			}
		}
	}

	private void dealWithResponse(CollectCoordinationResponse<Row> response, long responseOffset) {
		String responseVersion = response.getVersion();
		long responseLastCheckpointedOffset = response.getLastCheckpointedOffset();
		List<Row> results;
		try {
			results = response.getResults(serializer);
		} catch (IOException e) {
			LOG.warn("An exception occurs when deserializing query results. Some results might be lost.", e);
			results = Collections.emptyList();
		}

		if (responseLastCheckpointedOffset > lastCheckpointedOffset) {
			// a new checkpoint happens
			checkpointResults(responseLastCheckpointedOffset - lastCheckpointedOffset);
			lastCheckpointedOffset = responseLastCheckpointedOffset;
		}

		if (!version.equals(responseVersion)) {
			// sink restarted
			throwAwayUncheckpointedResults(offset - responseLastCheckpointedOffset);
			version = responseVersion;
			offset = responseLastCheckpointedOffset;
		}

		if (!results.isEmpty()) {
			int addStart = (int) (offset - responseOffset);
			List<Row> addedResults = results.subList(addStart, results.size());
			addToUncheckpointedBuffer(addedResults);
			offset += addedResults.size();
		}
	}

	public void close() {
		if (closed) {
			return;
		}

		cancelJob();

		try {
			if (checkpointedIterator != null) {
				checkpointedIterator.close();
				checkpointedIterator = null;
			}
			uncheckpointedBuffer.close();
			checkpointedBuffer.close();

			memoryManager.shutdown();
			ioManager.close();
		} catch (Exception e) {
			LOG.warn("Error when closing ResettableExternalBuffers", e);
		}

		closed = true;
	}

	@Override
	protected void finalize() throws Throwable {
		// in case that user neither reads all data nor closes the iterator
		close();
	}

	private ResettableExternalBuffer newBuffer() {
		return new ResettableExternalBuffer(
			ioManager,
			new LazyMemorySegmentPool(this, memoryManager, pageNum),
			new BinaryRowDataSerializer(1),
			// we're not using newBuffer(beginRow) so this is OK
			false);
	}

	private void addToUncheckpointedBuffer(List<Row> results) {
		for (Row result : results) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			try {
				serializer.serialize(result, wrapper);
				reuseWriter.writeBinary(0, baos.toByteArray());
				reuseWriter.complete();
				uncheckpointedBuffer.add(reuseRow);
				reuseWriter.reset();
			} catch (IOException e) {
				// this shouldn't be possible
				LOG.warn(
					"An error occurs when serializing from ResettableExternalBuffer. Some data might be lost.", e);
			}
		}
	}

	private void checkpointResults(long checkpointCount) {
		Preconditions.checkState(
			checkpointCount >= 0,
			"checkpointCount must be non-negative. This is a bug.");
		if (checkpointCount == 0) {
			return;
		}

		if (checkpointCount == uncheckpointedBuffer.size()) {
			checkpointWholeBuffer();
		} else {
			try {
				checkpointPartialBuffer(checkpointCount);
			} catch (IOException e) {
				LOG.warn("An error occurs in checkpointPartialBuffer. Some data might be lost.", e);
			}
		}
	}

	private void checkpointWholeBuffer() {
		ResettableExternalBuffer oldCheckpointedBuffer = checkpointedBuffer;

		// close last checkpointed results first
		if (checkpointedIterator != null) {
			checkpointedIterator.close();
		}
		checkpointedBuffer.reset();

		uncheckpointedBuffer.complete();
		checkpointedBuffer = uncheckpointedBuffer;
		checkpointedIterator = checkpointedBuffer.newIterator();

		uncheckpointedBuffer = oldCheckpointedBuffer;
	}

	private void checkpointPartialBuffer(long checkpointCount) throws IOException {
		// close last checkpointed results first
		if (checkpointedIterator != null) {
			checkpointedIterator.close();
		}
		checkpointedBuffer.reset();

		// move `checkpointCount` results to checkpointedBuffer
		uncheckpointedBuffer.complete();
		ResettableExternalBuffer.BufferIterator uncheckpointedIterator = uncheckpointedBuffer.newIterator();
		for (int i = 0; i < checkpointCount; i++) {
			uncheckpointedIterator.advanceNext();
			checkpointedBuffer.add(uncheckpointedIterator.getRow());
		}
		checkpointedBuffer.complete();
		checkpointedIterator = checkpointedBuffer.newIterator();

		swapUncheckpointedWithStandBy(uncheckpointedIterator);
	}

	private void checkpointFinalResults() {
		// add all results in checkpointed and uncheckpointed buffers
		try {
			if (checkpointedIterator != null) {
				while (checkpointedIterator.advanceNext()) {
					standByBuffer.add(checkpointedIterator.getRow());
				}
				checkpointedIterator.close();
			}

			uncheckpointedBuffer.complete();
			ResettableExternalBuffer.BufferIterator uncheckpointedIterator = uncheckpointedBuffer.newIterator();
			while (uncheckpointedIterator.advanceNext()) {
				standByBuffer.add(uncheckpointedIterator.getRow());
			}
			uncheckpointedIterator.close();
		} catch (IOException e) {
			LOG.warn("An error occurs in checkpointFinalResults. Some data might be lost.", e);
		}

		standByBuffer.complete();
		checkpointedIterator = standByBuffer.newIterator();

		ResettableExternalBuffer oldStandByBuffer = standByBuffer;
		standByBuffer = checkpointedBuffer;
		checkpointedBuffer = oldStandByBuffer;
	}

	private void throwAwayUncheckpointedResults(long throwAwayCount) {
		Preconditions.checkState(
			throwAwayCount >= 0,
			"throwAwayCount must be non-negative. This is a bug.");
		if (throwAwayCount == 0) {
			return;
		}

		if (throwAwayCount == uncheckpointedBuffer.size()) {
			uncheckpointedBuffer.reset();
		} else {
			try {
				throwAwayPartialBuffer(throwAwayCount);
			} catch (IOException e) {
				LOG.warn("An error occurs in throwAwayPartialBuffer. Some data might be lost.", e);
			}
		}
	}

	private void throwAwayPartialBuffer(long throwAwayCount) throws IOException {
		// skip `throwAwayCount` results
		uncheckpointedBuffer.complete();
		ResettableExternalBuffer.BufferIterator uncheckpointedIterator = uncheckpointedBuffer.newIterator();
		for (int i = 0; i < throwAwayCount; i++) {
			uncheckpointedIterator.advanceNext();
		}

		swapUncheckpointedWithStandBy(uncheckpointedIterator);
	}

	private void swapUncheckpointedWithStandBy(
			ResettableExternalBuffer.BufferIterator uncheckpointedIterator) throws IOException {
		// move remaining results to another uncheckpointed buffer
		while (uncheckpointedIterator.advanceNext()) {
			standByBuffer.add(uncheckpointedIterator.getRow());
		}

		// close current uncheckpointed buffer
		uncheckpointedIterator.close();
		uncheckpointedBuffer.reset();

		// swap two uncheckpointed buffers
		ResettableExternalBuffer oldUncheckpointedBuffer = uncheckpointedBuffer;
		uncheckpointedBuffer = standByBuffer;
		standByBuffer = oldUncheckpointedBuffer;
	}

	private List<Row> getNextCheckpointedResult() {
		List<Row> ret;

		if (checkpointedIterator == null) {
			// checkpointed results hasn't been initialized,
			// which means that no more checkpointed results have occurred
			ret = Collections.emptyList();
		} else if (checkpointedIterator.advanceNext()) {
			BinaryRowData binaryRow = checkpointedIterator.getRow();
			byte[] bytes = binaryRow.getBinary(0);
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
			try {
				Row result = serializer.deserialize(wrapper);
				ret = Collections.singletonList(result);
			} catch (IOException e) {
				// this shouldn't be possible
				LOG.warn(
					"An error occurs when deserializing from ResettableExternalBuffer. Some data might be lost.", e);
				ret = Collections.emptyList();
			}
		} else {
			ret = Collections.emptyList();
		}

		if (terminated && ret.isEmpty()) {
			// no more results, close buffer and memory manager and we're done
			close();
		}

		return ret;
	}

	@SuppressWarnings("unchecked")
	private CollectCoordinationResponse<Row> sendRequest(
			String version,
			long offset) throws InterruptedException, ExecutionException {
		checkJobClientConfigured();
		CoordinationRequestGateway gateway = (CoordinationRequestGateway) jobClient;

		OperatorID operatorId = operatorIdFuture.getNow(null);
		Preconditions.checkNotNull(operatorId, "Unknown operator ID. This is a bug.");

		CollectCoordinationRequest request = new CollectCoordinationRequest(version, offset);
		return (CollectCoordinationResponse) gateway.sendCoordinationRequest(operatorId, request).get();
	}

	private Tuple2<Long, CollectCoordinationResponse> getAccumulatorResults() {
		checkJobClientConfigured();

		JobExecutionResult executionResult;
		try {
			// this timeout is sort of hack, see comments in isJobTerminated for explanation
			executionResult = jobClient.getJobExecutionResult(getClass().getClassLoader()).get(
				DEFAULT_ACCUMULATOR_GET_MILLIS, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException("Failed to fetch job execution result", e);
		}

		ArrayList<byte[]> accResults = executionResult.getAccumulatorResult(accumulatorName);
		if (accResults == null) {
			// job terminates abnormally
			try {
				return Tuple2.of(offset, new CollectCoordinationResponse<>(
					version, lastCheckpointedOffset, Collections.emptyList(), serializer));
			} catch (IOException e) {
				// this is impossible
				throw new RuntimeException("Failed to create empty collect response when job terminates abnormally", e);
			}
		}

		try {
			List<byte[]> serializedResults =
				SerializedListAccumulator.deserializeList(accResults, BytePrimitiveArraySerializer.INSTANCE);
			byte[] serializedResult = serializedResults.get(0);
			return CollectSinkFunction.deserializeAccumulatorResult(serializedResult);
		} catch (ClassNotFoundException | IOException e) {
			// this is impossible
			throw new RuntimeException("Failed to deserialize accumulator results", e);
		}
	}

	private boolean isJobTerminated() {
		checkJobClientConfigured();

		try {
			JobStatus status = jobClient.getJobStatus().get();
			return status.isGloballyTerminalState();
		} catch (Exception e) {
			// TODO
			//  This is sort of hack.
			//  Currently different execution environment will have different behaviors
			//  when fetching a finished job status.
			//  For example, standalone session cluster will return a normal FINISHED,
			//  while mini cluster will throw IllegalStateException,
			//  and yarn per job will throw ApplicationNotFoundException.
			//  We have to assume that job has finished in this case.
			//  Change this when these behaviors are unified.
			LOG.warn("Failed to get job status so we assume that the job has terminated. Some data might be lost.", e);
			return true;
		}
	}

	private void cancelJob() {
		checkJobClientConfigured();

		if (!isJobTerminated()) {
			jobClient.cancel();
		}
	}

	private void sleepBeforeRetry() {
		if (retryMillis <= 0) {
			return;
		}

		try {
			// TODO a more proper retry strategy?
			Thread.sleep(retryMillis);
		} catch (InterruptedException e) {
			LOG.warn("Interrupted when sleeping before a retry", e);
		}
	}

	private void checkJobClientConfigured() {
		Preconditions.checkNotNull(jobClient, "Job client must be configured before first use.");
	}
}
