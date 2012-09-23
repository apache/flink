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

package eu.stratosphere.nephele.rpc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This message is used to transport an exception of a remote procedure call back to the caller.
 * <p>
 * This message is in general not thread-safe.
 * 
 * @author warneke
 */
final class RPCThrowable extends RPCResponse implements KryoSerializable {

	/**
	 * The exception to be transported.
	 */
	private Throwable throwable;

	/**
	 * Constructs a new RPC exception message.
	 * 
	 * @param messageID
	 *        the message ID
	 * @param throwable
	 *        the exception to be transported
	 */
	RPCThrowable(final int messageID, final Throwable throwable) {
		super(messageID);

		this.throwable = throwable;
	}

	/**
	 * The default constructor required by kryo.
	 */
	private RPCThrowable() {
		super(0);

		this.throwable = null;
	}

	/**
	 * Returns the transported exception.
	 * 
	 * @return the transported exception
	 */
	Throwable getThrowable() {
		return this.throwable;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {
		super.write(kryo, output);

		output.writeString(this.throwable.getClass().getName());
		kryo.writeObject(output, this.throwable);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	public void read(final Kryo kryo, final Input input) {
		super.read(kryo, input);

		Class<? extends Throwable> clazz = null;
		try {
			clazz = (Class<? extends Throwable>) Class.forName(input.readString());
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException(cnfe);
		}

		this.throwable = kryo.readObject(input, clazz);
	}
}
