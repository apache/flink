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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
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

	public static final int MINIMUM_NUMBER_OF_BUFFERS = 2;
	
	public static final int MIN_BUFFER_SIZE = 16 * 1024;
	
	// ------------------------------------------------------------------------

	protected final MemoryManager memoryManager;

	protected final IOManager ioManager;

	protected final Reader<T> recordReader;
	
	protected final RecordDeserializer<T> deserializer;

	protected final List<MemorySegment> memorySegments;

	private ArrayList<Buffer.Input> inputBuffers;

	private ChannelReader ioReader;

	private Channel.ID bufferID;
	
	private T next;
	
	private final int numBuffers;

	private int currentBuffer;

	private int usedBuffers;

	private boolean fitsIntoMem;
	
	private int count = 0;
	
	private volatile boolean abortFlag = false;
	
	// ------------------------------------------------------------------------

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
			long availableMemory, RecordDeserializer<T> deserializer, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.recordReader = reader;
		this.deserializer = deserializer;

		// allocate memory segments and open IO Buffers on them
		this.memorySegments = this.memoryManager.allocate(parentTask, availableMemory, MINIMUM_NUMBER_OF_BUFFERS, MIN_BUFFER_SIZE);
		this.numBuffers = this.memorySegments.size();
		
		this.currentBuffer = 0;
		
		LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
	}
	
	/**
	 * Constructs a new <tt>ResettableIterator</tt>
	 * 
	 * @param memoryManager
	 * @param ioManager
	 * @param reader
	 * @param availableMemory
	 * @throws MemoryAllocationException
	 */
	public SpillingResettableIterator(MemoryManager memoryManager, IOManager ioManager, final Iterator<T> it,
			long availableMemory, RecordDeserializer<T> deserializer, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.recordReader = new Reader<T>() {

			@Override
			public List<AbstractInputChannel<T>> getInputChannels() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public T next() throws IOException, InterruptedException {
				T next = it.next();
				return next;
			}
			
		};
		
		this.deserializer = deserializer;

		// allocate memory segments and open IO Buffers on them
		this.memorySegments = this.memoryManager.allocate(parentTask, availableMemory, MINIMUM_NUMBER_OF_BUFFERS, MIN_BUFFER_SIZE);
		this.numBuffers = this.memorySegments.size();
		
		this.currentBuffer = 0;
		
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
	public void open() throws ServiceException, IOException, InterruptedException
	{
		fitsIntoMem = true;
		
		// create output Buffers around the memory segments
		ArrayList<Buffer.Output> outputBuffers = new ArrayList<Buffer.Output>(this.numBuffers);
		for (MemorySegment segment : memorySegments) {
			Buffer.Output out = new Buffer.Output(segment);
			outputBuffers.add(out);
		}
		
		// try to read data into memory
		T nextRecord = null;
		while (this.recordReader.hasNext() && !this.abortFlag) {
			nextRecord = this.recordReader.next();
			count++;
			if (!outputBuffers.get(currentBuffer).write(nextRecord)) {
				// buffer is full, switch to next buffer
				currentBuffer++;
				if (currentBuffer == this.numBuffers) {
					fitsIntoMem = false;
					break;
				}
				outputBuffers.get(currentBuffer).write(nextRecord);
			}
		}

		// if the data does not fit into memory, we have to open a FileChannel
		if (!fitsIntoMem) {
			bufferID = this.ioManager.createChannel();
			// read in elements and serialize them into the buffer
			ChannelWriter writer = ioManager.createChannelWriter(bufferID, outputBuffers, true);
			// serialize the unwritten element
			writer.write(nextRecord);
			while (recordReader.hasNext() && !this.abortFlag) {
				count++;
				writer.write(recordReader.next());
			}
			writer.close();
			// now open a reader on the channel
			ioReader = ioManager.createChannelReader(bufferID, memorySegments, false);
			
			LOG.debug("Iterator opened, serialized " + count + " objects to disk.");
		} else {
			usedBuffers = currentBuffer + 1;
			inputBuffers = new ArrayList<Buffer.Input>(this.numBuffers);
			// create input Buffers on the output Buffers
			for (Buffer.Output out : outputBuffers) {
				int offset = out.getPosition();
				MemorySegment segment = out.dispose();
				Buffer.Input in = new Buffer.Input(segment);
				in.reset(offset);
				inputBuffers.add(in);
			}
			currentBuffer = 0;
			LOG.debug("Iterator opened, serialized " + count + " objects to memory.");
		}
		count = 0;
		
		if(this.abortFlag) {
			this.close();
		}
	}

	public void reset() {
		try {
			this.next = null;
			if (fitsIntoMem) {
				if (currentBuffer != usedBuffers)
					inputBuffers.get(currentBuffer).rewind();
				currentBuffer = 0;
			} else {
				ioReader.close();
				ioReader = ioManager.createChannelReader(bufferID, memorySegments, false); // reopen
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		LOG.debug("Iterator reset, deserialized " + count + " objects in previous run.");
		count = 0;
	}

	@Override
	public boolean hasNext() {
		if (this.next == null) {
			this.next = deserializer.getInstance();
			if (fitsIntoMem) {
				if (currentBuffer == usedBuffers)
					return false;
				
				// try to read data
				if (!inputBuffers.get(currentBuffer).read(this.next)) {
					// switch to next buffer
					inputBuffers.get(currentBuffer).rewind(); // reset the old buffer
					currentBuffer++;
					if (currentBuffer == usedBuffers) { 
						// we are depleted
						this.next = null;
						return false;
					}
					
					// read the next element from the new buffer
					inputBuffers.get(currentBuffer).read(this.next);
				}
				return true;
			} else {
				try {
					if(ioReader.read(this.next)) {
						return true;
					} else {
						this.next = null;
						return false;
					}
				}
				catch (IOException ioex) {
					throw new RuntimeException(ioex);
				}
			}
		} else {
			return true;
		}
	}

	@Override
	public T next() {
		if(this.next == null) {
			if(!hasNext()) {
				return null;
			}
		}
		++count;
		final T out = this.next;
		this.next = null;
		return out;
	}

	public void close() {
		if (ioReader != null) {
			try {
				ioReader.close();
				ioReader.deleteChannel();
			}
			catch (IOException ioex) {
				throw new RuntimeException();
			}
		}
		memoryManager.release(memorySegments);
		LOG.debug("Iterator closed. Deserialized " + count + " objects in last run.");
	}
	
	public void abort() {
		this.abortFlag = true;
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
