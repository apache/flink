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

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.ChannelPersistenceITCase.getStreamFactoryFactory;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * {@link ChannelStateWriterImpl} lifecycle tests.
 */
public class ChannelStateWriterImplTest {
	private static final long CHECKPOINT_ID = 42L;
	private static final String TASK_NAME = "test";

	@Test(expected = IllegalArgumentException.class)
	public void testAddEventBuffer() throws Exception {

		NetworkBuffer dataBuf = getBuffer();
		NetworkBuffer eventBuf = getBuffer();
		eventBuf.setDataType(Buffer.DataType.EVENT_BUFFER);
		try {
			runWithSyncWorker(writer -> {
				callStart(writer);
				writer.addInputData(CHECKPOINT_ID, new InputChannelInfo(1, 1), 1, ofElements(Buffer::recycleBuffer, eventBuf, dataBuf));
			});
		} finally {
			assertTrue(dataBuf.isRecycled());
		}
	}

	@Test
	public void testResultCompletion() throws IOException {
		ChannelStateWriteResult result;
		try (ChannelStateWriterImpl writer = openWriter()) {
			callStart(writer);
			result = writer.getWriteResult(CHECKPOINT_ID);
			ChannelStateWriteResult result2 = writer.getWriteResult(CHECKPOINT_ID);
			assertSame(result, result2);
			assertFalse(result.resultSubpartitionStateHandles.isDone());
			assertFalse(result.inputChannelStateHandles.isDone());
		}
		assertTrue(result.inputChannelStateHandles.isDone());
		assertTrue(result.resultSubpartitionStateHandles.isDone());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testResultCleanup() throws IOException {
		try (ChannelStateWriterImpl writer = openWriter()) {
			callStart(writer);
			writer.getWriteResult(CHECKPOINT_ID);
			writer.stop(CHECKPOINT_ID);
			writer.getWriteResult(CHECKPOINT_ID);
		}
	}

	@Test
	public void testAbort() throws Exception {
		NetworkBuffer buffer = getBuffer();
		runWithSyncWorker((writer, worker) -> {
			callStart(writer);
			ChannelStateWriteResult result = writer.getWriteResult(CHECKPOINT_ID);
			callAddInputData(writer, buffer);
			callAbort(writer);
			worker.processAllRequests();
			assertTrue(result.isDone());
			assertTrue(buffer.isRecycled());
		});
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAbortClearsResults() throws Exception {
		NetworkBuffer buffer = getBuffer();
		runWithSyncWorker((writer, worker) -> {
			callStart(writer);
			callAbort(writer);
			worker.processAllRequests();
			writer.getWriteResult(CHECKPOINT_ID);
		});
	}

	@Test
	public void testAbortIgnoresMissing() throws Exception {
		runWithSyncWorker(this::callAbort);
	}

	@Test(expected = TestException.class)
	public void testBuffersRecycledOnError() throws Exception {
		unwrappingError(TestException.class, () -> {
			NetworkBuffer buffer = getBuffer();
			try (ChannelStateWriterImpl writer = new ChannelStateWriterImpl(
					TASK_NAME,
					new ConcurrentHashMap<>(),
					failingWorker(),
					5)) {
				writer.open();
				callAddInputData(writer, buffer);
			} finally {
				assertTrue(buffer.isRecycled());
			}
		});
	}

	@Test
	public void testBuffersRecycledOnClose() throws Exception {
		NetworkBuffer buffer = getBuffer();
		runWithSyncWorker(writer -> {
			callStart(writer);
			callAddInputData(writer, buffer);
			assertFalse(buffer.isRecycled());
		});
		assertTrue(buffer.isRecycled());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoAddDataAfterFinished() throws Exception {
		unwrappingError(IllegalArgumentException.class, () -> runWithSyncWorker(
			writer -> {
				callStart(writer);
				callFinish(writer);
				callAddInputData(writer);
			}
		));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddDataNotStarted() throws Exception {
		unwrappingError(IllegalArgumentException.class, () -> runWithSyncWorker(writer -> callAddInputData(writer)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFinishNotStarted() throws Exception {
		unwrappingError(IllegalArgumentException.class, () -> runWithSyncWorker(this::callFinish));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRethrowOnClose() throws Exception {
		unwrappingError(IllegalArgumentException.class, () -> runWithSyncWorker(
			writer -> {
				try {
					callFinish(writer);
				} catch (IllegalArgumentException e) {
					// ignore here - should rethrow in close
				}
			}
		));
	}

	@Test(expected = TestException.class)
	public void testRethrowOnNextCall() throws Exception {
		SyncChannelStateWriteRequestExecutor worker = new SyncChannelStateWriteRequestExecutor();
		ChannelStateWriterImpl writer = new ChannelStateWriterImpl(TASK_NAME, new ConcurrentHashMap<>(), worker, 5);
		writer.open();
		worker.setThrown(new TestException());
		unwrappingError(TestException.class, () -> callStart(writer));
	}

	@Test
	public void testStartAbortsOldCheckpoints() throws Exception {
		int maxCheckpoints = 10;
		runWithSyncWorker((writer, worker) -> {
			writer.start(0, CheckpointOptions.forCheckpointWithDefaultLocation());
			ChannelStateWriteResult writeResult = writer.getWriteResult(0);
			for (int i = 1; i <= maxCheckpoints; i++) {
				writer.start(i, CheckpointOptions.forCheckpointWithDefaultLocation());
				worker.processAllRequests();
				assertTrue(writeResult.isDone());
				writeResult = writer.getWriteResult(i);
			}
		});
	}

	@Test(expected = IllegalStateException.class)
	public void testStartNotOpened() throws Exception {
		unwrappingError(IllegalStateException.class, () -> {
			try (ChannelStateWriterImpl writer = new ChannelStateWriterImpl(TASK_NAME, getStreamFactoryFactory())) {
				callStart(writer);
			}
		});
	}

	@Test(expected = IllegalStateException.class)
	public void testNoStartAfterClose() throws Exception {
		unwrappingError(IllegalStateException.class, () -> {
			ChannelStateWriterImpl writer = openWriter();
			writer.close();
			writer.start(42, CheckpointOptions.forCheckpointWithDefaultLocation());
		});
	}

	@Test(expected = IllegalStateException.class)
	public void testNoAddDataAfterClose() throws Exception {
		unwrappingError(IllegalStateException.class, () -> {
			ChannelStateWriterImpl writer = openWriter();
			callStart(writer);
			writer.close();
			callAddInputData(writer);
		});
	}

	private static <T extends Throwable> void unwrappingError(Class<T> clazz, RunnableWithException r) throws Exception {
		try {
			r.run();
		} catch (Exception e) {
			throw findThrowable(e, clazz).map(te -> (Exception) te).orElse(e);
		}
	}

	private NetworkBuffer getBuffer() {
		return new NetworkBuffer(HeapMemorySegment.FACTORY.allocateUnpooledSegment(123, null), FreeingBufferRecycler.INSTANCE);
	}

	private ChannelStateWriteRequestExecutor failingWorker() {
		return new ChannelStateWriteRequestExecutor() {
			@Override
			public void close() {
			}

			@Override
			public void submit(ChannelStateWriteRequest e) {
				throw new TestException();
			}

			@Override
			public void submitPriority(ChannelStateWriteRequest e) {
				throw new TestException();
			}

			@Override
			public void start() throws IllegalStateException {
			}
		};
	}

	private void runWithSyncWorker(Consumer<ChannelStateWriter> writerConsumer) throws Exception {
		runWithSyncWorker((channelStateWriter, syncChannelStateWriterWorker) -> writerConsumer.accept(channelStateWriter));
	}

	private void runWithSyncWorker(BiConsumerWithException<ChannelStateWriter, SyncChannelStateWriteRequestExecutor, Exception> testFn) throws Exception {
		try (
				SyncChannelStateWriteRequestExecutor worker = new SyncChannelStateWriteRequestExecutor();
				ChannelStateWriterImpl writer = new ChannelStateWriterImpl(TASK_NAME, new ConcurrentHashMap<>(), worker, 5)
		) {
			writer.open();
			testFn.accept(writer, worker);
			worker.processAllRequests();
		}
	}

	private ChannelStateWriterImpl openWriter() {
		ChannelStateWriterImpl writer = new ChannelStateWriterImpl(TASK_NAME, getStreamFactoryFactory());
		writer.open();
		return writer;
	}

	private void callStart(ChannelStateWriter writer) {
		writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());
	}

	private void callAddInputData(ChannelStateWriter writer, NetworkBuffer... buffer) {
		writer.addInputData(CHECKPOINT_ID, new InputChannelInfo(1, 1), 1, ofElements(Buffer::recycleBuffer, buffer));
	}

	private void callAbort(ChannelStateWriter writer) {
		writer.abort(CHECKPOINT_ID, new TestException());
	}

	private void callFinish(ChannelStateWriter writer) {
		writer.finishInput(CHECKPOINT_ID);
		writer.finishOutput(CHECKPOINT_ID);
	}

}

class TestException extends RuntimeException {
}

class SyncChannelStateWriteRequestExecutor implements ChannelStateWriteRequestExecutor {
	private final ChannelStateWriteRequestDispatcher requestProcessor;
	private final Deque<ChannelStateWriteRequest> deque;
	private Exception thrown;

	SyncChannelStateWriteRequestExecutor() {
		deque = new ArrayDeque<>();
		requestProcessor = new ChannelStateWriteRequestDispatcherImpl(getStreamFactoryFactory(), new ChannelStateSerializerImpl());
	}

	@Override
	public void submit(ChannelStateWriteRequest e) throws Exception {
		deque.offer(e);
		if (thrown != null) {
			throw thrown;
		}
	}

	@Override
	public void submitPriority(ChannelStateWriteRequest e) throws Exception {
		deque.offerFirst(e);
		if (thrown != null) {
			throw thrown;
		}
	}

	@Override
	public void start() throws IllegalStateException {
	}

	@Override
	public void close() {
	}

	void processAllRequests() throws Exception {
		while (!deque.isEmpty()) {
			requestProcessor.dispatch(deque.poll());
		}
	}

	public void setThrown(Exception thrown) {
		this.thrown = thrown;
	}
}

