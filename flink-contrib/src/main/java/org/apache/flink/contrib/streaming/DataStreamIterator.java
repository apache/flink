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

package org.apache.flink.contrib.streaming;

import java.util.Iterator;
import java.net.ServerSocket;
import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.io.DataInputStream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;

class DataStreamIterator<T> implements Iterator<T> {

	ServerSocket socket;
	InputStream tcpStream;
	T next;
	private final CountDownLatch connectionAccepted = new CountDownLatch(1);
	private volatile StreamReaderDataInputView streamReader;
	private final TypeSerializer<T> serializer;

	DataStreamIterator(TypeSerializer serializer) {
		this.serializer = serializer;
		try {
			socket = new ServerSocket(0, 1, null);
		} catch (IOException e) {
			throw new RuntimeException("DataStreamIterator: an I/O error occurred when opening the socket", e);
		}
		(new AcceptThread()).start();
	}

	private class AcceptThread extends Thread {
		public void run() {
			try {
				tcpStream = socket.accept().getInputStream();
				streamReader = new StreamReaderDataInputView(tcpStream);
				connectionAccepted.countDown();
			} catch (IOException e) {
				throw new RuntimeException("DataStreamIterator.AcceptThread failed", e);
			}
		}
	}

	/**
	 * Returns the port on which the iterator is getting the data. (Used internally.)
	 * @return The port
	 */
	public int getPort() {
		return socket.getLocalPort();
	}

	/**
	 * Returns true if the DataStream has more elements.
	 * (Note: blocks if there will be more elements, but they are not available yet.)
	 * @return true if the DataStream has more elements
	 */
	@Override
	public boolean hasNext() {
		if (next == null) {
			readNextFromStream();
		}
		return next != null;
	}

	/**
	 * Returns the next element of the DataStream. (Blocks if it is not available yet.)
	 * @return The element
	 * @throws NoSuchElementException if the stream has already ended
	 */
	@Override
	public T next() {
		if (next == null) {
			readNextFromStream();
			if (next == null) {
				throw new NoSuchElementException();
			}
		}
		T current = next;
		next = null;
		return current;
	}

	private void readNextFromStream(){
		try {
			connectionAccepted.await();
		} catch (InterruptedException e) {
			throw new RuntimeException("The calling thread of DataStreamIterator.readNextFromStream was interrupted.");
		}
		try {
			next = serializer.deserialize(streamReader);
		} catch (EOFException e) {
			next = null;
		} catch (IOException e) {
			throw new RuntimeException("DataStreamIterator could not read from deserializedStream", e);
		}
	}

	private static class StreamReaderDataInputView extends DataInputStream implements DataInputView {

		public StreamReaderDataInputView(InputStream stream) {
			super(stream);
		}

		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
