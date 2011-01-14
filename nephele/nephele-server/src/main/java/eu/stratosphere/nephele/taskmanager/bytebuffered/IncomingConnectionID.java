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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.net.InetAddress;

/**
 * This class is used to identify an incoming data connection. It is used for data connections from the network and file
 * checkpoints. In case the incoming connection reads data from the network the incoming connection ID is backed by the
 * address of host the data is sent from. If the incoming connection reads data from a checkpoint the ID is backed by
 * the name of the first checkpoint file.
 * 
 * @author warneke
 */
public class IncomingConnectionID {

	/**
	 * The address of the source host in case the incoming connection reads from the network.
	 */
	private final InetAddress inetAddress;

	/**
	 * The name of the first checkpoint file in case the incoming connection reads from a checkpoint.
	 */
	private final String fileName;

	/**
	 * Constructs a new incoming connection ID for network connection.
	 * 
	 * @param inetAddress
	 *        the address of the source host
	 */
	public IncomingConnectionID(InetAddress inetAddress) {

		if (inetAddress == null) {
			throw new IllegalArgumentException("Provided InetAddress must not be null");
		}

		this.inetAddress = inetAddress;
		this.fileName = null;
	}

	/**
	 * Constructs a new incoming connection ID for a checkpoint
	 * 
	 * @param fileName
	 *        the name of the first checkpoint file
	 */
	public IncomingConnectionID(String fileName) {

		if (fileName == null) {
			throw new IllegalArgumentException("Provided file name must not be null");
		}

		this.inetAddress = null;
		this.fileName = fileName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (this.inetAddress != null) {
			return this.inetAddress.equals(obj);
		} else {
			return this.fileName.equals(obj);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.inetAddress != null) {
			return this.inetAddress.hashCode();
		} else {
			return this.fileName.hashCode();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		
		if (this.inetAddress != null) {
			return this.inetAddress.toString();
		} else {
			return this.fileName.toString();
		}
	}
}
