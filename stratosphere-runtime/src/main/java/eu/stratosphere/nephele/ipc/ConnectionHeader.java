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

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;

/**
 * The IPC connection header sent by the client to the server
 * on connection establishment.
 */
class ConnectionHeader implements IOReadableWritable {

	private String protocol;

	public ConnectionHeader() {
	}

	/**
	 * Creates a new {@link ConnectionHeader} with the given <code>protocol</code>.
	 * 
	 * @param protocol
	 *        protocol used for communication between the IPC client and the server
	 */
	public ConnectionHeader(String protocol) {
		this.protocol = protocol;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.protocol = StringRecord.readString(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		StringRecord.writeString(out, this.protocol);
	}

	public String getProtocol() {
		return this.protocol;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return this.protocol;
	}
}
