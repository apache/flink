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

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReader;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.runtime.task.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.task.util.ResettableIterator;

/**
 * Implementation of a resettable iterator, that reads all data from a given
 * channel into primary/secondary storage and allows resetting the iterator
 * 
 * @author mheimel
 * @param <T>
 */
public class SpillingResettableIterator<T extends Record> implements ResettableIterator<T>, LastRepeatableIterator<T> {

	private static final Log LOG = LogFactory.getLog(SpillingResettableIterator.class);

	protected static final int nrOfBuffers = 2;
	
	
	private int count = 0;

	protected MemoryManager memoryManager;

	protected IOManager ioManager;

	protected Reader<T> recordReader;

	

	protected Vector<MemorySegment> memorySegments;

	protected Vector<Buffer.Input> inputBuffers;

	protected ChannelReader ioReader;

	protected Channel.ID bufferID;
	
	protected T next;
	
	protected RecordDeserializer<T> deserializer;

	protected int currentBuffer;

	protected int usedBuffers;

	protected boolean fitsIntoMem;

	/**
	 * Constructs a new <tt>ResettableIterator</tt>
	 * 
	 * @param memoryManager
	 * @param ioManager
	 * @param reader
	 * @param availableMemory
	 * @throws MemoryAllocationException
	 */
	public SpillingResettableIterator(MemoryManager memoryManager, IOManager ioManager, Reader<T> reader,
			int availableMemory, RecordDeserializer<T> deserializer, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.recordReader = reader;
		this.deserializer = deserializer;

		// allocate memory segments and open IO Buffers on them
		memorySegments = new Vector<MemorySegment>(nrOfBuffers);
		for (int i = 0; i < nrOfBuffers; ++i)
			memorySegments.add(this.memoryManager.allocate(parentTask, availableMemory / nrOfBuffers));
		currentBuffer = 0;
		LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
	}

	/**
	 * Open the iterator. This will serialize the complete content of the specified Reader<T> into a file and initialize
	 * the ResettableIterator to this File.
	 * 
	 * @throws ServiceException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void open() throws ServiceException, IOException, InterruptedException {

		fitsIntoMem = true;
		// allocate output Buffers on the memory segments
		Vector<Buffer.Output> outputBuffers = new Vector<Buffer.Output>(nrOfBuffers);
		for (MemorySegment segment : memorySegments) {
			Buffer.Output out = new Buffer.Output();
			out.bind(segment);
			outputBuffers.add(out);
		}
		// try to read data into memory
		while (recordReader.hasNext()) {
			next = recordReader.next();
			count++;
			if (!outputBuffers.get(currentBuffer).write(next)) {
				// buffer is full, switch to next buffer
				currentBuffer++;
				if (currentBuffer == nrOfBuffers) {
					fitsIntoMem = false;
					break;
				}
				outputBuffers.get(currentBuffer).write(next);
			}
		}

		// if the data does not fit into memory, we have to open a FileChannel
		if (!fitsIntoMem) {
			bufferID = this.ioManager.createChannel();
			// read in elements and serialize them into the buffer
			ChannelWriter writer = ioManager.createChannelWriter(bufferID, outputBuffers, true);
			// serialize the unwritten element
			writer.write(next);
			while (recordReader.hasNext()) {
				count++;
				writer.write(recordReader.next());
			}
			writer.close();
			// now open a reader on the channel
			ioReader = ioManager.createChannelReader(bufferID, memorySegments);
			
			LOG.debug("Iterator opened, serialized " + count + " objects to disk.");
		} else {
			usedBuffers = currentBuffer + 1;
			inputBuffers = new Vector<Buffer.Input>(nrOfBuffers);
			// create input Buffers on the output Buffers
			for (Buffer.Output out : outputBuffers) {
				int offset = out.getPosition();
				MemorySegment segment = out.unbind();
				Buffer.Input in = new Buffer.Input();
				in.bind(segment);
				in.reset(offset);
				inputBuffers.add(in);
			}
			currentBuffer = 0;
			LOG.debug("Iterator opened, serialized " + count + " objects to memory.");
		}
		count = 0;
		next = null;
	}

	public void reset() {
		try {
			next = null;
			if (fitsIntoMem) {
				if (currentBuffer != usedBuffers)
					inputBuffers.get(currentBuffer).reset();
				currentBuffer = 0;
			} else {
				ioReader.close();
				ioReader = ioManager.createChannelReader(bufferID, memorySegments); // reopen
			}
		} catch (ServiceException e) {
			throw new RuntimeException(e);
		}
		LOG.debug("Iterator reset, deserialized " + count + " objects in previous run.");
		count = 0;
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			next = deserializer.getInstance();
			if (fitsIntoMem) {
				if (currentBuffer == usedBuffers)
					return false;
				
				// try to read data
				if (!inputBuffers.get(currentBuffer).read(next)) {
					// switch to next buffer
					inputBuffers.get(currentBuffer).reset(); // reset the old buffer
					currentBuffer++;
					if (currentBuffer == usedBuffers) // we are depleted
						return false;
					
					// read the next element from the new buffer
					inputBuffers.get(currentBuffer).read(next);
				}
				return true;
			} else {
				
				return ioReader.read(next);
			}
		} else {
			return true;
		}
	}

	@Override
	public T next() {
		if(next == null) {
			if(!hasNext()) {
				return null;
			}
		}
		++count;
		final T out = next;
		next = null;
		return out;
	}

	public void close() throws ServiceException {
		if (!fitsIntoMem) {
			ioReader.close();
		}
		memoryManager.release(memorySegments);
		LOG.debug("Iterator closed. Deserialized " + count + " objects in last run.");
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public T repeatLast() {

		T lastReturned = null;
		lastReturned = deserializer.getInstance();
		
		if(fitsIntoMem) {
			inputBuffers.get(currentBuffer).repeatRead(lastReturned);
		} else {
			ioReader.repeatRead(lastReturned);
		}
		
		return lastReturned;
	}

}
