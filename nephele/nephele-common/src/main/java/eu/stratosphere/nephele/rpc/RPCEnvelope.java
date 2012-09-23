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
 * An RPC envelope is the basic data structure which wraps all other types of messages for the RPC communication. Its
 * basic purpose is to provide a common entry point for serialization and to separate most of the serialization code
 * from the actual RPC logic.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 */
class RPCEnvelope implements KryoSerializable {

	/**
	 * The actual RPC message to be transported.
	 */
	private RPCMessage rpcMessage;

	/**
	 * Constructs a new RPC envelope and wraps the given message.
	 * 
	 * @param rpcMessage
	 *        the message to be wrapped
	 */
	RPCEnvelope(final RPCMessage rpcMessage) {

		this.rpcMessage = rpcMessage;
	}

	/**
	 * The default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private RPCEnvelope() {
		this.rpcMessage = null;
	}

	/**
	 * Returns the wrapped RPC message.
	 * 
	 * @return the wrapped RPC message
	 */
	RPCMessage getRPCMessage() {
		return this.rpcMessage;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		if (this.rpcMessage instanceof RPCRequest) {
			output.writeByte(0);
		} else if (this.rpcMessage instanceof RPCReturnValue) {
			output.writeByte(1);
		} else if (this.rpcMessage instanceof RPCThrowable) {
			output.writeByte(2);
		} else if (this.rpcMessage instanceof RPCCleanup) {
			output.writeByte(3);
		}

		kryo.writeObject(output, this.rpcMessage);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		final byte type = input.readByte();
		switch (type) {
		case 1:
			this.rpcMessage = kryo.readObject(input, RPCReturnValue.class);
			break;
		case 2:
			this.rpcMessage = kryo.readObject(input, RPCThrowable.class);
			break;
		case 3:
			this.rpcMessage = kryo.readObject(input, RPCCleanup.class);
			break;
		default:
			this.rpcMessage = kryo.readObject(input, RPCRequest.class);
			break;
		}
	}
}
