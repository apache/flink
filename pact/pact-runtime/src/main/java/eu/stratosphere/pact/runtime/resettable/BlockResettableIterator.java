/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.resettable;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.runtime.task.util.MemoryBlockIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers a resettable
 * iterator on this block.
 * 
 * @author mheimel
 * @param <T>
 */
public class BlockResettableIterator<T extends Record> implements MemoryBlockIterator<T> {

	private static final Log LOG = LogFactory.getLog(BlockResettableIterator.class);

	protected MemoryManager memoryManager;

	protected Vector<MemorySegment> buffers;

	protected T deserializationInstance = null;

	protected boolean allRead = false;

	protected int readingPosition = 0;

	protected int limit = 0;

	protected RecordDeserializer<T> deserializer = null;

	protected Buffer.Input in;

	protected BlockingQueue<MemorySegment> emptySegments;

	protected BlockingQueue<Buffer.Input> filledBuffers;

	protected BlockFetcher<T> blockFetcher;

	protected Thread blockFetcherThread;

	public BlockResettableIterator(MemoryManager memoryManager, Reader<T> reader, int availableMemory, int nrOfBuffers,
			RecordDeserializer<T> deserializer)
												throws MemoryAllocationException {
		this.deserializer = deserializer;
		this.memoryManager = memoryManager;
		// allocate the queues
		emptySegments = new LinkedBlockingQueue<MemorySegment>();
		filledBuffers = new LinkedBlockingQueue<Buffer.Input>();
		// allocate the memory buffers
		buffers = new Vector<MemorySegment>(nrOfBuffers);
		for (int i = 0; i < nrOfBuffers; ++i)
			buffers.add(memoryManager.allocate(availableMemory / nrOfBuffers));
		// now append all memory segments to the workerQueue
		emptySegments.addAll(buffers);
		// create the writer thread
		blockFetcher = new BlockFetcher<T>(emptySegments, filledBuffers, reader);
		LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
	}

	@Override
	public boolean hasNext() {
		if (deserializationInstance == null) {
			deserializationInstance = deserializer.getInstance();
			boolean result = in.read(deserializationInstance);
			if (result == false)
				deserializationInstance = null;
			return result;
		}
		return false;
	}

	@Override
	public T next() {
		T out = deserializationInstance;
		deserializationInstance = null;
		return out;
	}

	public void reset() {
		// re-open the input reader
		in.reset();
		deserializationInstance = null;
	}

	public boolean nextBlock() {
		// add the last block to the worker queue of the writer thread
		if (in != null)
			emptySegments.add(in.unbind());
		// now fetch the latest filled Buffer
		try {
			in = filledBuffers.take();
		} catch (InterruptedException e) {
			throw new RuntimeException("BlockResettableIterator: Unable to fetch the last filled buffer", e);
		}
		if (!in.isBound())
			return false;
		return true;
	}

	public void open() {
		LOG.debug("Iterator opened.");
		// start the writer Thread
		blockFetcherThread = new Thread(blockFetcher);
		blockFetcherThread.start();
		// fetch the first block
		nextBlock();
	}

	public void close() throws ServiceException {
		// release the memory segment
		memoryManager.release(buffers);
		LOG.debug("Iterator closed.");
	}

	@Override
	public void remove() {
		// do nothing
	}

	protected class BlockFetcher<R extends Record> implements Runnable {

		protected BlockingQueue<MemorySegment> requestQueue;

		protected BlockingQueue<Buffer.Input> finishedTasks;

		protected Reader<R> reader;

		protected R next = null;

		public BlockFetcher(BlockingQueue<MemorySegment> inputQueue, BlockingQueue<Buffer.Input> outputQueue,
				Reader<R> recordReader) {
			this.requestQueue = inputQueue;
			this.finishedTasks = outputQueue;
			this.reader = recordReader;
		}

		@Override
		public void run() {
			boolean finished = false;

			while (!finished) {
				// wait for the next request
				MemorySegment request = null;
				try {
					request = requestQueue.take();
				} catch (InterruptedException e1) {
					throw new RuntimeException("BlockResettableIterator: Unable to take next request", e1);
				}
				// create an output buffer
				Buffer.Output out = new Buffer.Output();
				out.bind(request);

				// write the last spilled element
				if (next != null)
					out.write(next);

				// now fetch elements from the reader until the memory segment is filled
				finished = true;
				while (reader.hasNext()) {
					try {
						next = reader.next();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					if (!out.write(next)) {
						finished = false; // there are elements remaining
						break;
					}
				}

				// allocate a new input buffer for the segment and push it to the input queue
				Buffer.Input in = new Buffer.Input();
				in.bind(request);
				in.reset(out.getPosition());
				out.unbind();
				finishedTasks.add(in);
			}
			Buffer.Input in = new Buffer.Input();
			finishedTasks.add(in); // unbound buffer signals completion
		}

	}

}
