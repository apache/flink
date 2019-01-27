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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;
import org.apache.flink.streaming.runtime.tasks.InputSelector;
import org.apache.flink.streaming.runtime.tasks.InputSelector.EdgeInputSelection;
import org.apache.flink.streaming.runtime.tasks.InputSelector.SourceInputSelection;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link StreamArbitraryInputProcessor}.
 */
public class StreamArbitraryInputProcessorTest {

	@Test(expected = NoSuchElementException.class)
	public void testEnqueueDequeueInputFetchers() {
		final StreamArbitraryInputProcessor processor = new StreamArbitraryInputProcessor(
			mock(IOManager.class),
			this,
			mock(InputSelector.class),
			mock(TaskMetricGroup.class),
			mock(SelectedReadingBarrierHandler.class));

		final InputFetcher inputFetcher1 = new FakeInputFetcher(processor, processor);
		final InputFetcher inputFetcher2 = new FakeInputFetcher(processor, processor);

		processor.enqueueInputFetcher(inputFetcher1);
		processor.enqueueInputFetcher(inputFetcher2);

		assertEquals(2, processor.getEnqueuedInputFetchers().size());
		assertEquals(inputFetcher1, processor.dequeueInputFetcher());
		assertEquals(inputFetcher2, processor.dequeueInputFetcher());
		assertTrue(processor.getEnqueuedInputFetchers().isEmpty());
		// Will cause a NoSuchElementException
		assertNull(processor.dequeueInputFetcher());
	}

	@Test
	public void testConstructInputFetcherReadingQueue() {
		final InputSelector.InputSelection inputSelection1 = SourceInputSelection.create(1);
		final InputSelector.InputSelection inputSelection2 = SourceInputSelection.create(2);
		final InputFetcher inputFetcher1 = mock(InputFetcher.class);
		when(inputFetcher1.getInputSelection()).thenReturn(inputSelection1);
		when(inputFetcher1.moreAvailable()).thenReturn(false);
		final InputFetcher inputFetcher2 = mock(InputFetcher.class);
		when(inputFetcher2.getInputSelection()).thenReturn(inputSelection2);
		when(inputFetcher2.moreAvailable()).thenReturn(true);

		final FakeInputSelector inputSelector = new FakeInputSelector();
		inputSelector.add(Collections.singletonList(inputSelection2));

		final StreamArbitraryInputProcessor processor = new StreamArbitraryInputProcessor(
			mock(IOManager.class),
			this,
			inputSelector,
			mock(TaskMetricGroup.class),
			mock(SelectedReadingBarrierHandler.class));

		processor.getInputFetchers().add(inputFetcher2);
		processor.getInputFetchers().add(inputFetcher1);

		processor.constructInputFetcherReadingQueue();

		assertEquals(1, processor.getInputFetcherReadingQueue().size());
		assertEquals(1, processor.getEnqueuedInputFetchers().size());

		assertEquals(inputFetcher2, processor.getInputFetcherReadingQueue().peek());
		assertTrue(processor.getEnqueuedInputFetchers().contains(inputFetcher2));
		assertFalse(processor.getEnqueuedInputFetchers().contains(inputFetcher1));
	}

	@Test
	public void testEmptySourceProcess() throws Exception {

		final InputSelector.InputSelection inputSelection = SourceInputSelection.create(1);

		final FakeInputSelector inputSelector = new FakeInputSelector();
		inputSelector.add(Collections.singletonList(inputSelection));

		final StreamArbitraryInputProcessor processor = new StreamArbitraryInputProcessor(
			mock(IOManager.class),
			this,
			inputSelector,
			mock(TaskMetricGroup.class),
			mock(SelectedReadingBarrierHandler.class));

		final SourceFunctionV2<String> sourceFunction = new SourceFunctionV2<String>() {
			@Override
			public boolean isFinished() {
				return true;
			}

			@Override
			public SourceRecord<String> next() throws Exception {
				return null;
			}

			@Override
			public void cancel() {

			}
		};

		final EndInputChecker endInputChecker = new EndInputChecker();
		final StreamSourceV2 sourceOperator = new StreamSourceV2<>(sourceFunction);
		processor.bindSourceOperator(
			1,
			sourceOperator,
			endInputChecker,
			new FakeSourceContext(),
			mock(StreamStatusSubMaintainer.class));

		processor.process();

		assertTrue(endInputChecker.endInputInvoked);
	}

	@Test
	public void testProcess() throws Exception {
		final FakeInputSelector inputSelector = new FakeInputSelector();
		final StreamArbitraryInputProcessor processor = new StreamArbitraryInputProcessor(
			mock(IOManager.class),
			this,
			inputSelector,
			mock(TaskMetricGroup.class),
			mock(SelectedReadingBarrierHandler.class));

		/*
		 1. sourceFetcher1 process return true
		 2. sourceFetcher1 process return false
		 3. sourceFetcher1 is not finished, more available is false
		 4. sourceFetcher2 process return false
		 5. sourceFetcher2 is finished
		 6. selection changed to input gate fetcher
		 7. inputGeteFetcher1 process return false
		 8. inputGateFetcher1 is finished
		 7. inputGeteFetcher2 process return false, trigger available of sourceFetcher1
		 8. inputGateFetcher2 is finished
		 9. input selection changed to source fetcher
		 10.continue fetch sourceFetcher1, wait available listener wake this up
		 11.sourceFetcher1 is finished
		 12.all fetchers are finished
		*/
		final FakeInputFetcher sourceFetcher1 = new FakeInputFetcher(processor, processor);
		sourceFetcher1.processResults.add(true);
		sourceFetcher1.processResults.add(false);
		sourceFetcher1.processResults.add(false);
		sourceFetcher1.isFinishedResults.add(false);
		sourceFetcher1.isFinishedResults.add(true);
		sourceFetcher1.inputSelection = SourceInputSelection.create(0);
		sourceFetcher1.moreAvailableResults.add(true);
		sourceFetcher1.moreAvailableResults.add(false);
		sourceFetcher1.moreAvailableResults.add(true);

		final FakeInputFetcher sourceFetcher2 = new FakeInputFetcher(processor, processor);
		sourceFetcher2.processResults.add(false);
		sourceFetcher2.isFinishedResults.add(true);
		sourceFetcher2.inputSelection = SourceInputSelection.create(3);
		sourceFetcher2.moreAvailableResults.add(true);
		sourceFetcher2.triggerSelectionChangedWhenProcessing(1);

		processor.getInputFetchers().add(sourceFetcher1);
		processor.getInputFetchers().add(sourceFetcher2);

		final FakeInputFetcher inputGateFetcher1 = new FakeInputFetcher(processor, processor);
		final StreamEdge streamEdge1 = mock(StreamEdge.class);
		final FakeInputFetcher inputGateFetcher2 = new FakeInputFetcher(processor, processor);
		final StreamEdge streamEdge2 = mock(StreamEdge.class);

		inputGateFetcher1.processResults.add(false);
		inputGateFetcher1.isFinishedResults.add(true);
		inputGateFetcher1.inputSelection = EdgeInputSelection.create(streamEdge1);
		inputGateFetcher1.moreAvailableResults.add(true);

		inputGateFetcher2.processResults.add(false);
		inputGateFetcher2.isFinishedResults.add(true);
		inputGateFetcher2.inputSelection = EdgeInputSelection.create(streamEdge2);
		inputGateFetcher2.moreAvailableResults.add(true);
		inputGateFetcher2.triggerSelectionChangedWhenProcessing(1);
		inputGateFetcher2.triggerInputFetcherAvailableWhenProcessing(1, sourceFetcher1);

		processor.getInputFetchers().add(inputGateFetcher1);
		processor.getInputFetchers().add(inputGateFetcher2);

		final List<InputSelector.InputSelection> inputSelections1 = new ArrayList<>();
		inputSelections1.add(SourceInputSelection.create(0));
		inputSelections1.add(SourceInputSelection.create(3));

		final List<InputSelector.InputSelection> inputSelections2 = new ArrayList<>();
		inputSelections2.add(EdgeInputSelection.create(streamEdge1));
		inputSelections2.add(EdgeInputSelection.create(streamEdge2));

		inputSelector.add(inputSelections1);
		inputSelector.add(inputSelections2);
		inputSelector.add(inputSelections1);

		processor.process();

		assertTrue(sourceFetcher1.processResults.isEmpty());
		assertTrue(sourceFetcher2.processResults.isEmpty());
		assertTrue(inputGateFetcher1.processResults.isEmpty());
		assertTrue(inputGateFetcher2.processResults.isEmpty());

		assertTrue(sourceFetcher1.isFinishedResults.isEmpty());
		assertTrue(sourceFetcher2.isFinishedResults.isEmpty());
		assertTrue(inputGateFetcher1.isFinishedResults.isEmpty());
		assertTrue(inputGateFetcher2.isFinishedResults.isEmpty());

		assertTrue(processor.getInputFetchers().isEmpty());

		assertTrue(sourceFetcher1.doSetUp);
		assertTrue(sourceFetcher2.doSetUp);
		assertTrue(inputGateFetcher1.doSetUp);
		assertTrue(inputGateFetcher2.doSetUp);
		assertTrue(sourceFetcher1.doCleanUp);
		assertTrue(sourceFetcher2.doCleanUp);
		assertTrue(inputGateFetcher1.doCleanUp);
		assertTrue(inputGateFetcher2.doCleanUp);
	}

	private class FakeInputSelector implements InputSelector {

		private Queue<List<InputSelection>> inputSelections = new ArrayDeque<>();

		private SelectionChangedListener listener;

		void add(List<InputSelection> inputSelection) {
			inputSelections.add(inputSelection);
		}

		@Override
		public void registerSelectionChangedListener(SelectionChangedListener listener) {
			this.listener = listener;
		}

		@Override
		public List<InputSelection> getNextSelectedInputs() {
			return inputSelections.remove();
		}

		public void triggerSelectionChanged() {
			assertNotNull(listener);
			listener.notifySelectionChanged();
		}
	}

	private class FakeInputFetcher implements InputFetcher, InputSelector {

		public final Queue<Boolean> processResults = new ArrayDeque<>();

		public final Queue<Boolean> isFinishedResults = new ArrayDeque<>();

		public final Queue<Boolean> moreAvailableResults = new ArrayDeque<>();

		public InputSelector.InputSelection inputSelection;

		public boolean doCleanUp = false;

		public boolean doSetUp = false;

		private InputFetcherAvailableListener availableListener;

		private int triggerSelectionChanged = Integer.MAX_VALUE;

		private SelectionChangedListener selectionChangedListener;

		private int triggerAvalalable = Integer.MAX_VALUE;

		private InputFetcher availableInputFetcher;

		public FakeInputFetcher(InputFetcherAvailableListener availableListener, SelectionChangedListener selectionChangedListener) {
			registerAvailableListener(availableListener);
			registerSelectionChangedListener(selectionChangedListener);
		}

		public void triggerSelectionChangedWhenProcessing(int afterProcessingTimes) {
			triggerSelectionChanged = afterProcessingTimes;
		}

		public void triggerInputFetcherAvailableWhenProcessing(int afterProcessingTimes, InputFetcher availableInputFetcher) {
			triggerAvalalable = afterProcessingTimes;
			this.availableInputFetcher = availableInputFetcher;
		}

		@Override
		public void setup() throws Exception {
			assertFalse(doSetUp);
			doSetUp = true;
		}

		@Override
		public boolean fetchAndProcess() throws Exception {
			final boolean result = processResults.remove();
			if (--triggerSelectionChanged <= 0) {
				if (selectionChangedListener != null) {
					selectionChangedListener.notifySelectionChanged();
					triggerSelectionChanged = Integer.MAX_VALUE;
				}
			}
			if (--triggerAvalalable <= 0) {
				if (availableListener != null && availableInputFetcher != null) {
					final ExecutorService singleThreadPool = Executors.newSingleThreadExecutor();
					singleThreadPool.execute(() -> {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
						availableListener.notifyInputFetcherAvailable(availableInputFetcher);
						triggerAvalalable = Integer.MAX_VALUE;
					});
				}
			}
			return result;
		}

		@Override
		public boolean isFinished() {
			return isFinishedResults.remove();
		}

		@Override
		public boolean moreAvailable() {
			return moreAvailableResults.remove();
		}

		@Override
		public void cleanup() {
			assertFalse(doCleanUp);
			doCleanUp = true;
		}

		@Override
		public void cancel() {

		}

		@Override
		public InputSelector.InputSelection getInputSelection() {
			return inputSelection;
		}

		@Override
		public void registerAvailableListener(InputFetcherAvailableListener listener) {
			this.availableListener = listener;
		}

		@Override
		public void registerSelectionChangedListener(SelectionChangedListener listener) {
			this.selectionChangedListener = listener;
		}

		@Override
		public List<InputSelection> getNextSelectedInputs() {
			return null;
		}
	}

	private class EndInputChecker extends AbstractUdfStreamOperator<String, Function> implements OneInputStreamOperator<String, String> {
		public boolean endInputInvoked = false;

		public EndInputChecker() {
			super(new Function() {});
		}

		@Override
		public void processElement(StreamRecord element) throws Exception {

		}

		@Override
		public void endInput() throws Exception {
			assertFalse(endInputInvoked);
			endInputInvoked = true;
		}
	}

	private class FakeSourceContext implements SourceFunction.SourceContext {

		@Override
		public void collect(Object element) {

		}

		@Override
		public void collectWithTimestamp(Object element, long timestamp) {

		}

		@Override
		public void emitWatermark(Watermark mark) {

		}

		@Override
		public void markAsTemporarilyIdle() {

		}

		@Override
		public Object getCheckpointLock() {
			return this;
		}

		@Override
		public void close() {

		}
	}
}

