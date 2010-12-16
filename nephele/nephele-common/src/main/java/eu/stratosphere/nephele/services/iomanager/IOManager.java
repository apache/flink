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

package eu.stratosphere.nephele.services.iomanager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.ChannelReader.ReaderThread;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter.WriterThread;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;

/**
 * The facade for the provided IO manager services.
 * 
 * @author Alexander Alexandrov
 */
public final class IOManager {
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(IOManager.class);

	/**
	 * The default temp path for anonymous Channels.
	 */
	private final String path;

	/**
	 * A random number generator for the anonymous ChannelIDs.
	 */
	private final Random random;

	/**
	 * The writer thread used for asynchronous block oriented channel writing.
	 */
	private final WriterThread writer;

	/**
	 * The reader thread used for asynchronous block oriented channel reading.
	 */
	private final ReaderThread reader;

	/**
	 * A boolean flag indicating whether the close() has already been invoked.
	 */
	private boolean isClosed = false;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public IOManager() {
		this(System.getProperty("java.io.tmpdir"));
	}

	/**
	 * Constructs a new IOManager.
	 * 
	 * @param path
	 *        the basic directory path for files underlying anonymous
	 *        channels.
	 */
	public IOManager(String path) {
		LOG.info("creating DefaultIOManager instance");

		this.path = path;
		this.random = new Random();
		this.writer = new ChannelWriter.WriterThread();
		this.reader = new ChannelReader.ReaderThread();

		// start the ChannelWriter worker thread
		Thread writerThread = new Thread(this.writer);
		writerThread.setName("IOManager writer thread");
		writerThread.start();

		// start the ChannelReader worker thread
		Thread readerThread = new Thread(this.reader);
		readerThread.setName("IOManager reader thread");
		readerThread.start();
	}

	/**
	 * Close method.
	 */
	public final void shutdown() {
		if (!isClosed) {
			LOG.info("Closing DefaultIOManager instance.");

			writer.close();
			reader.close();

			isClosed = true;
		}
	}

	/**
	 * Creates a new {@link Channel.ID} in the default {@code path}.
	 * 
	 * @return
	 */
	public Channel.ID createChannel() {
		return createChannel(path);
	}

	/**
	 * Creates a new {@link Channel.ID} in the specified {@code path}.
	 * 
	 * @param path
	 * @return
	 */
	public Channel.ID createChannel(String path) {
		return new Channel.ID(path, random);
	}

	/**
	 * Creates a new {@link Channel.Enumerator} in the default {@code path}.
	 * 
	 * @return
	 */
	public Channel.Enumerator createChannelEnumerator() {
		return createChannelEnumerator(path);
	}

	/**
	 * Creates a new {@link Channel.Enumerator} in the specified {@code path}.
	 * 
	 * @param path
	 * @return
	 */
	public Channel.Enumerator createChannelEnumerator(String path) {
		return new Channel.Enumerator(path, random);
	}

	/**
	 * <p>
	 * Creates a ChannelWriter for the anonymous file identified by the specified {@code channelID} using the provided
	 * {@code freeSegmens} as backing memory for an internal flow of output buffers.
	 * </p>
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @return
	 * @throws ServiceException
	 */
	public ChannelWriter createChannelWriter(Channel.ID channelID, Collection<MemorySegment> freeSegments)
			throws ServiceException {
		return new ChannelWriter(channelID, writer.requestQueue, IOManager.createBuffer(Buffer.Type.OUTPUT,
			freeSegments), false);
	}

	/**
	 * <p>
	 * Creates a ChannelWriter for the anonymous file identified by the specified {@code channelID} using the provided
	 * {@code memorySegments} as backing memory for an internal flow of output buffers. If the boolean variable {@code
	 * filled} is set, the content of the memorySegments is flushed to the file before reusing.
	 * </p>
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @param filled
	 * @return
	 * @throws ServiceException
	 */
	public ChannelWriter createChannelWriter(Channel.ID channelID, Collection<Buffer.Output> buffers, boolean filled)
			throws ServiceException {
		return new ChannelWriter(channelID, writer.requestQueue, buffers, filled);
	}

	/**
	 * <p>
	 * Creates a ChannelWriter for the anonymous file written on secondary storage and identified by the specified
	 * {@code channelID} using the provided {@code freeSegments} as backing memory for an internal flow of input
	 * buffers.
	 * </p>
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @return
	 * @throws ServiceException
	 */
	public ChannelReader createChannelReader(Channel.ID channelID, Collection<MemorySegment> freeSegments)
			throws ServiceException {
		return new ChannelReader(channelID, reader.requestQueue, freeSegments);
	}

	/**
	 * <p>
	 * Generic factory method for different buffer types. Please, be aware that the factory constructs <i>unbound</i>
	 * buffers. Binding the buffer to an underlying memory segment must be done by the client.
	 * </p>
	 * 
	 * @param <T>
	 * @param bufferType
	 *        the type of the buffer to be created
	 * @return T an unbound buffer from the specified type
	 * @throws ServiceException
	 */
	public static <T extends Buffer> T createBuffer(Buffer.Type<T> bufferType) throws ServiceException {
		try {
			return bufferType.clazz.newInstance();
		} catch (InstantiationException e) {
			throw new IllegalArgumentException("Unknown buffer type", e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException("Unknown buffer type", e);
		}
	}

	/**
	 * <p>
	 * Generic factory method for typed initialized collections of different buffer types.
	 * </p>
	 * 
	 * @param <T>
	 * @param bufferType
	 * @param numberOfBuffers
	 * @return Collection<T> an unsynchronized collection of initialized buffers
	 * @throws ServiceException
	 */
	public static <T extends Buffer> Collection<T> createBuffer(Buffer.Type<T> bufferType, int numberOfBuffers)
			throws ServiceException {
		Collection<T> buffers = new ArrayList<T>(numberOfBuffers);

		for (int i = 0; i < numberOfBuffers; i++) {
			try {
				buffers.add(createBuffer(bufferType));
			} catch (ServiceException e) {
				throw e;
			}
		}

		return buffers;
	}

	/**
	 * <p>
	 * Generic factory method for typed initialized collections of different buffer types.
	 * </p>
	 * 
	 * @param <T>
	 * @param bufferType
	 * @param numberOfBuffers
	 * @return Collection<T> an unsynchronized collection of initialized buffers
	 * @throws ServiceException
	 */
	public static <T extends Buffer> Collection<T> createBuffer(Buffer.Type<T> bufferType,
			Collection<MemorySegment> freeSegments) throws ServiceException {
		Collection<T> buffers = new ArrayList<T>(freeSegments.size());

		for (MemorySegment segment : freeSegments) {
			try {
				T buffer = createBuffer(bufferType);
				buffer.bind(segment);
				buffers.add(buffer);
			} catch (ServiceException e) {
				throw e;
			}
		}

		return buffers;
	}

	/**
	 * Unbinds the collection of IO buffers.
	 * 
	 * @param buffers
	 * @return a collection of the freed memory segments
	 * @throws UnboundMemoryBackedException
	 *         if the collection contains an
	 *         unbound buffer
	 */
	public static Collection<MemorySegment> unbindBuffers(BlockingQueue<? extends Buffer> buffers) {
		Collection<MemorySegment> freeSegments = new ArrayList<MemorySegment>(buffers.size());

		for (Buffer buffer : buffers) {
			freeSegments.add(buffer.unbind());
		}

		return freeSegments;
	}
}
