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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * An input that wraps an underlying input and sorts the incoming records. It starts emitting records
 * downstream only when all the other inputs coupled with this {@link MultiInputSortingDataInput} have
 * finished sorting as well.
 *
 * <p>Moreover it will report it is {@link #isAvailable() available} or
 * {@link #isApproximatelyAvailable() approximately available} if it has some records pending only if the head
 * of the {@link CommonContext#getQueueOfHeads()} belongs to the input. That way there is only ever one input
 * that reports it is available.
 *
 * <p>The sorter uses binary comparison of keys, which are extracted and serialized when received
 * from the chained input. Moreover the timestamps of incoming records are used for secondary ordering.
 * For the comparison it uses either {@link FixedLengthByteKeyComparator} if the length of the
 * serialized key is constant, or {@link VariableLengthByteKeyComparator} otherwise.
 *
 * <p>Watermarks, stream statuses, nor latency markers are propagated downstream as they do not make
 * sense with buffered records. The input emits the largest watermark seen after all records.
 */
public final class MultiInputSortingDataInput<IN, K> implements StreamTaskInput<IN> {
	private final int idx;
	private final StreamTaskInput<IN> wrappedInput;
	private final PushSorter<Tuple2<byte[], StreamRecord<IN>>> sorter;
	private final CommonContext commonContext;
	private final SortingPhaseDataOutput sortingPhaseDataOutput = new SortingPhaseDataOutput();

	private final KeySelector<IN, K> keySelector;
	private final TypeSerializer<K> keySerializer;
	private final DataOutputSerializer dataOutputSerializer;

	private MutableObjectIterator<Tuple2<byte[], StreamRecord<IN>>> sortedInput;
	private long seenWatermark = Long.MIN_VALUE;

	private MultiInputSortingDataInput(
			CommonContext commonContext,
			StreamTaskInput<IN> wrappedInput,
			int inputIdx,
			PushSorter<Tuple2<byte[], StreamRecord<IN>>> sorter,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			DataOutputSerializer dataOutputSerializer) {
		this.wrappedInput = wrappedInput;
		this.idx = inputIdx;
		this.commonContext = commonContext;
		this.sorter = sorter;
		this.keySelector = keySelector;
		this.keySerializer = keySerializer;
		this.dataOutputSerializer = dataOutputSerializer;
	}

	/**
	 * A wrapper that combines sorting {@link StreamTaskInput inputs} with a {@link InputSelectable} that should be
	 * used to choose which input to consume next from.
	 */
	public static class SelectableSortingInputs {
		private final InputSelectable inputSelectable;
		private final StreamTaskInput<?>[] sortingInputs;

		public SelectableSortingInputs(StreamTaskInput<?>[] sortingInputs, InputSelectable inputSelectable) {
			this.sortingInputs = sortingInputs;
			this.inputSelectable = inputSelectable;
		}

		public InputSelectable getInputSelectable() {
			return inputSelectable;
		}

		public StreamTaskInput<?>[] getSortingInputs() {
			return sortingInputs;
		}
	}

	public static <K> SelectableSortingInputs wrapInputs(
			AbstractInvokable containingTask,
			StreamTaskInput<Object>[] inputs,
			KeySelector<Object, K>[] keySelectors,
			TypeSerializer<Object>[] inputSerializers,
			TypeSerializer<K> keySerializer,
			MemoryManager memoryManager,
			IOManager ioManager,
			boolean objectReuse,
			double managedMemoryFraction,
			Configuration jobConfiguration) {
		int keyLength = keySerializer.getLength();
		final TypeComparator<Tuple2<byte[], StreamRecord<Object>>> comparator;
		DataOutputSerializer dataOutputSerializer;
		if (keyLength > 0) {
			dataOutputSerializer = new DataOutputSerializer(keyLength);
			comparator = new FixedLengthByteKeyComparator<>(keyLength);
		} else {
			dataOutputSerializer = new DataOutputSerializer(64);
			comparator = new VariableLengthByteKeyComparator<>();
		}

		int numberOfInputs = inputs.length;
		CommonContext commonContext = new CommonContext(numberOfInputs);
		StreamTaskInput<?>[] sortingInputs = IntStream.range(0, numberOfInputs)
			.mapToObj(
				idx -> {
					try {
						KeyAndValueSerializer<Object> keyAndValueSerializer = new KeyAndValueSerializer<>(
							inputSerializers[idx],
							keyLength);
						return new MultiInputSortingDataInput<>(
							commonContext,
							inputs[idx],
							idx,
							ExternalSorter.newBuilder(
								memoryManager,
								containingTask,
								keyAndValueSerializer,
								comparator)
								.memoryFraction(managedMemoryFraction / numberOfInputs)
								.enableSpilling(
									ioManager,
									jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
								.maxNumFileHandles(
									jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN) / numberOfInputs)
								.objectReuse(objectReuse)
								.largeRecords(true)
								.build(),
							keySelectors[idx],
							keySerializer,
							dataOutputSerializer
						);
					} catch (MemoryAllocationException e) {
						throw new RuntimeException();
					}
				}
			).toArray(StreamTaskInput[]::new);
		return new SelectableSortingInputs(
			sortingInputs,
			new InputSelector(
				commonContext,
				numberOfInputs
			)
		);
	}

	@Override
	public int getInputIndex() {
		return idx;
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) {
		throw new UnsupportedOperationException("Checkpoints are not supported with sorted inputs" +
			" in the BATCH runtime.");
	}

	@Override
	public void close() throws IOException {
		IOException ex = null;
		try {
			wrappedInput.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		try {
			sorter.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		if (ex != null) {
			throw ex;
		}
	}

	@Override
	public InputStatus emitNext(DataOutput<IN> output) throws Exception {
		if (sortedInput != null) {
			return emitNextAfterSorting(output);
		}

		InputStatus inputStatus = wrappedInput.emitNext(sortingPhaseDataOutput);
		if (inputStatus == InputStatus.END_OF_INPUT) {
			endSorting();
			return addNextToQueue(new HeadElement(idx), output);
		}

		return inputStatus;
	}

	@Nonnull
	@SuppressWarnings({"unchecked"})
	private InputStatus emitNextAfterSorting(DataOutput<IN> output) throws Exception {
		if (commonContext.isFinishedEmitting(idx)) {
			return InputStatus.END_OF_INPUT;
		} else if (commonContext.allSorted()) {
			HeadElement head = commonContext.getQueueOfHeads().peek();
			if (head != null && head.inputIndex == idx) {
				HeadElement headElement = commonContext.getQueueOfHeads().poll();
				output.emitRecord((StreamRecord<IN>) headElement.streamElement.f1);
				return addNextToQueue(headElement, output);
			} else {
				return InputStatus.NOTHING_AVAILABLE;
			}
		} else {
			return InputStatus.NOTHING_AVAILABLE;
		}
	}

	private void endSorting() throws Exception {
		sorter.finishReading();
		commonContext.setFinishedSorting(idx);
		sortedInput = sorter.getIterator();
		if (commonContext.allSorted()) {
			commonContext.getAllFinished().getUnavailableToResetAvailable().complete(null);
		}
	}

	@Nonnull
	private InputStatus addNextToQueue(HeadElement reuse, DataOutput<IN> output) throws Exception {
		Tuple2<byte[], StreamRecord<IN>> next = sortedInput.next();
		if (next != null) {
			reuse.streamElement = getAsObject(next);
			commonContext.getQueueOfHeads().add(reuse);
		} else {
			commonContext.setFinishedEmitting(idx);
			if (seenWatermark > Long.MIN_VALUE) {
				output.emitWatermark(new Watermark(seenWatermark));
			}
			return InputStatus.END_OF_INPUT;
		}

		if (commonContext.allSorted()) {
			HeadElement headElement = commonContext.getQueueOfHeads().peek();
			if (headElement != null) {
				if (headElement.inputIndex == idx) {
					return InputStatus.MORE_AVAILABLE;
				}
			}
		}

		return InputStatus.NOTHING_AVAILABLE;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private Tuple2<byte[], StreamRecord<Object>> getAsObject(Tuple2<byte[], StreamRecord<IN>> next) {
		return (Tuple2<byte[], StreamRecord<Object>>) (Tuple2) next;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (sortedInput != null) {
			return commonContext.getAllFinished().getAvailableFuture();
		} else {
			return wrappedInput.getAvailableFuture();
		}
	}

	// ---------------------------------------------------------------------------------
	//  Utility classes
	// ---------------------------------------------------------------------------------

	/**
	 * An {@link InputSelectable} that indicates which input contain the current smallest
	 * element of all sorting inputs. Should be used by the {@link StreamInputProcessor} to
	 * choose the next input to consume from.
	 */
	private static class InputSelector implements InputSelectable {

		private final CommonContext commonContext;
		private final int numberOfInputs;

		private InputSelector(CommonContext commonContext, int numberOfInputs) {
			this.commonContext = commonContext;
			this.numberOfInputs = numberOfInputs;
		}

		@Override
		public InputSelection nextSelection() {
			if (commonContext.allSorted()) {
				HeadElement headElement = commonContext.getQueueOfHeads().peek();
				if (headElement != null) {
					int headIdx = headElement.inputIndex;
					return new InputSelection.Builder().select(headIdx + 1).build(numberOfInputs);
				}
			}
			return InputSelection.ALL;
		}
	}

	/**
	 * A {@link PushingAsyncDataInput.DataOutput} used in the sorting phase when we have not seen all the
	 * records from the underlying input yet. It forwards the records to a corresponding sorter.
	 */
	private class SortingPhaseDataOutput implements PushingAsyncDataInput.DataOutput<IN> {

		@Override
		public void emitRecord(StreamRecord<IN> streamRecord) throws Exception {
			K key = keySelector.getKey(streamRecord.getValue());

			keySerializer.serialize(key, dataOutputSerializer);
			byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
			dataOutputSerializer.clear();

			sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			seenWatermark = Math.max(seenWatermark, watermark.getTimestamp());
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) {

		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {

		}
	}

	/**
	 * A thin wrapper that represents a head of a sorted input. Additionally it keeps the id of
	 * the input it belongs to.
	 *
	 * <p>The class is mutable and we only ever have a single instance per input.
	 */
	private static final class HeadElement implements Comparable<HeadElement> {
		final int inputIndex;
		Tuple2<byte[], StreamRecord<Object>> streamElement;

		private HeadElement(int inputIndex) {
			this.inputIndex = inputIndex;
		}

		@Override
		public int compareTo(HeadElement o) {
			int keyCmp = compare(streamElement.f0, o.streamElement.f0);
			if (keyCmp != 0) {
				return keyCmp;
			}
			return Long.compare(
				streamElement.f1.asRecord().getTimestamp(),
				o.streamElement.f1.asRecord().getTimestamp());
		}

		private int compare(byte[] first, byte[] second) {
			int firstLength = first.length;
			int secondLength = second.length;
			int minLength = Math.min(firstLength, secondLength);
			for (int i = 0; i < minLength; i++) {
				int cmp = Byte.compare(first[i], second[i]);

				if (cmp != 0) {
					return cmp;
				}
			}

			return Integer.compare(firstLength, secondLength);
		}
	}

	private static final class CommonContext {
		private final PriorityQueue<HeadElement> queueOfHeads = new PriorityQueue<>();
		private final AvailabilityProvider.AvailabilityHelper allFinished = new AvailabilityProvider.AvailabilityHelper();
		private long notFinishedSortingMask = 0;
		private long finishedEmitting = 0;

		public CommonContext(int numberOfInputs) {
			for (int i = 0; i < numberOfInputs; i++) {
				notFinishedSortingMask = setBitMask(notFinishedSortingMask, i);
			}
		}

		public boolean allSorted() {
			return notFinishedSortingMask == 0;
		}

		public void setFinishedSorting(int inputIndex) {
			this.notFinishedSortingMask = unsetBitMask(this.notFinishedSortingMask, inputIndex);
		}

		public void setFinishedEmitting(int inputIndex) {
			this.finishedEmitting = setBitMask(this.finishedEmitting, inputIndex);
		}

		public boolean isFinishedEmitting(int inputIndex) {
			return checkBitMask(this.finishedEmitting, inputIndex);
		}

		public PriorityQueue<HeadElement> getQueueOfHeads() {
			return queueOfHeads;
		}

		public AvailabilityHelper getAllFinished() {
			return allFinished;
		}

		private static long setBitMask(long mask, int inputIndex) {
			return mask | 1L << inputIndex;
		}

		private static long unsetBitMask(long mask, int inputIndex) {
			return mask & ~(1L << inputIndex);
		}

		private static boolean checkBitMask(long mask, int inputIndex) {
			return (mask & (1L << inputIndex)) != 0;
		}
	}
}
