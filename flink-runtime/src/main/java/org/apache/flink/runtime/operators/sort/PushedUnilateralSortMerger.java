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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

public class PushedUnilateralSortMerger<E> extends UnilateralSortMerger<E> {
	private static final Logger LOG = LoggerFactory.getLogger(PushedUnilateralSortMerger.class);

	private boolean firstRecord = true;
	private long bytesUntilSpilling;

	private CircularElement<E> currentBuffer;

	private boolean addingDone = false;

	public PushedUnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
									  MemoryManager memoryManager, List<MemorySegment> memory, IOManager ioManager,
									  AbstractInvokable parentTask, TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
									  int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
									  float startSpillingFraction, boolean noSpillingMemory, boolean handleLargeRecords,
									  boolean objectReuseEnabled, boolean enableAsyncMerging) throws IOException {
		super(sortedDataFileFactory, merger, memoryManager, memory, ioManager, /* pushed, no input */null, parentTask, serializerFactory,
			comparator, numSortBuffers, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, noSpillingMemory, handleLargeRecords,
			objectReuseEnabled, enableAsyncMerging);
	}

	@Override
	protected ThreadBase<E> getReadingThread(
		ExceptionHandler<IOException> exceptionHandler,
		MutableObjectIterator<E> reader,
		CircularQueues<E> queues,
		LargeRecordHandler<E> largeRecordHandler,
		AbstractInvokable parentTask,
		TypeSerializer<E> serializer,
		long startSpillingBytes) {
		return null;
	}

	public synchronized void add(E current) throws IOException {
		checkArgument(!addingDone, "Adding already done!");
		if (unhandledException != null) {
			throw unhandledException;
		}
		try {
			if (firstRecord) {
				this.bytesUntilSpilling = startSpillingBytes;

				if (this.bytesUntilSpilling < 1) {
					this.bytesUntilSpilling = 0;

					this.circularQueues.sort.add(spillingMarker());
				}

				firstRecord = false;
			}

			while (true) {
				// grab the next buffer
				while (currentBuffer == null) {
					try {
						currentBuffer = circularQueues.empty.take();

						if (LOG.isDebugEnabled()) {
							LOG.debug("Retrieved empty read buffer " + currentBuffer.id + ".");
						}

						if (!currentBuffer.buffer.isEmpty()) {
							throw new IOException("New buffer is not empty.");
						}
					} catch (InterruptedException iex) {
						throw new IOException(iex);
					}
				}

				final InMemorySorter<E> buffer = currentBuffer.buffer;

				if (!buffer.write(current)) {
					if (buffer.isEmpty()) {
						// did not fit in a fresh buffer, must be large...
						if (largeRecordHandler != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
							}
							largeRecordHandler.addRecord(current);
						} else {
							throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
								+ buffer.getCapacity() + " bytes).");
						}

						buffer.reset();
						break;
					} else {
						// buffer is full, send the buffer
						if (LOG.isDebugEnabled()) {
							LOG.debug("Emitting full buffer: " + currentBuffer.id + ".");
						}

						if (bytesUntilSpilling > 0) {
							bytesUntilSpilling -= buffer.getCapacity();

							if (bytesUntilSpilling <= 0) {
								bytesUntilSpilling = 0;
								// send the spilling marker
								final CircularElement<E> SPILLING_MARKER = spillingMarker();
								this.circularQueues.sort.add(SPILLING_MARKER);
							}
						}

						circularQueues.sort.add(currentBuffer);
						currentBuffer = null;
						// continue to process current record.
					}
				} else {
					if (bytesUntilSpilling > 0) {
						if (bytesUntilSpilling - buffer.getOccupancy() <= 0) {
							bytesUntilSpilling = 0;

							// send the spilling marker
							final CircularElement<E> SPILLING_MARKER = spillingMarker();
							this.circularQueues.sort.add(SPILLING_MARKER);
						}
					}

					break;
				}
			}
		} catch (Throwable e) {
			if (unhandledException != null) {
				LOG.warn("Record add failed.", e);
				throw unhandledException;
			} else {
				throw new IOException(e);
			}
		}
	}

	public synchronized void finishAdding() {
		if (!addingDone) {
			if (currentBuffer != null) {
				circularQueues.sort.add(currentBuffer);
			}

			final CircularElement<E> EOF_MARKER = endMarker();
			circularQueues.sort.add(EOF_MARKER);

			LOG.info("Sending done.");
			addingDone = true;
		}
	}
}
