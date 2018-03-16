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

package org.apache.flink.api.java.typeutils.runtime;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import org.apache.flink.annotation.Internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

@Internal
public class NoFetchingInput extends Input {
	public NoFetchingInput(InputStream inputStream){
		super(inputStream, 8);
	}

	@Override
	public boolean eof(){
		throw new UnsupportedOperationException("NoFetchingInput does not support EOF.");
	}

	@Override
	public int read() throws KryoException {
		require(1);
		return buffer[position++] & 0xFF;
	}

	@Override
	public boolean canReadInt() throws KryoException {
		throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
	}

	@Override
	public boolean canReadLong() throws KryoException {
		throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
	}

	/**
	 * Require makes sure that at least required number of bytes are kept in the buffer. If not, then
	 * it will load exactly the difference between required and currently available number of bytes.
	 * Thus, it will only load the data which is required and never prefetch data.
	 *
	 * @param required the number of bytes being available in the buffer
	 * @return the number of bytes remaining, which is equal to required
	 * @throws KryoException
	 */
	@Override
	protected int require(int required) throws KryoException {
		if(required > capacity) {
			throw new KryoException("Buffer too small: capacity: " + capacity + ", " +
					"required: " + required);
		}

		position = 0;
		int bytesRead = 0;
		int count;
		while(true){
			count = fill(buffer, bytesRead, required - bytesRead);

			if(count == -1){
				throw new KryoException(new EOFException("No more bytes left."));
			}

			bytesRead += count;
			if(bytesRead == required){
				break;
			}
		}
		limit = required;
		return required;
	}

	@Override
	public int read(byte[] bytes, int offset, int count) throws KryoException {
		if(bytes == null){
			throw new IllegalArgumentException("bytes cannot be null.");
		}

		try {
			return inputStream.read(bytes, offset, count);
		}catch(IOException ex){
			throw new KryoException(ex);
		}
	}

	@Override
	public void skip(int count) throws KryoException {
		try{
			inputStream.skip(count);
		}catch(IOException ex){
			throw new KryoException(ex);
		}
	}

	@Override
	public void readBytes(byte[] bytes, int offset, int count) throws KryoException {
		if(bytes == null){
			throw new IllegalArgumentException("bytes cannot be null.");
		}

		try{
			int bytesRead = 0;
			int c;

			while(true){
				c = inputStream.read(bytes, offset+bytesRead, count-bytesRead);

				if(c == -1){
					throw new KryoException(new EOFException("No more bytes left."));
				}

				bytesRead += c;

				if(bytesRead == count){
					break;
				}
			}
		}catch(IOException ex){
			throw new KryoException(ex);
		}
	}


}

