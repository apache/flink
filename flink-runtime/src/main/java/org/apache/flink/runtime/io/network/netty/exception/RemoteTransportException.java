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

package org.apache.flink.runtime.io.network.netty.exception;

import org.apache.flink.runtime.execution.CancelTaskException;

import java.net.SocketAddress;

/**
 * Exception thrown on remote transport failures.
 *
 * <p>If you get this type of exception at task manager T, it means that
 * something went wrong at the network stack of another task manager (not T).
 * It is not an issue at the task, which throws the Exception.
 */
public class RemoteTransportException extends CancelTaskException {

	private static final long serialVersionUID = 4373615529545893089L;

	/** Address of the remote task manager that caused this Exception. */
	private final SocketAddress remoteAddress;

	public RemoteTransportException() {
		this(null, null, null);
	}

	public RemoteTransportException(String msg, SocketAddress remoteAddress) {
		this(msg, null, remoteAddress);
	}

	public RemoteTransportException(String msg, Throwable cause, SocketAddress remoteAddress) {
		super(msg, cause);
		this.remoteAddress = remoteAddress;
	}

	/**
	 * Returns the address of the task manager causing this Exception.
	 *
	 * @return Address of the remote task manager causing this Exception
	 */
	public SocketAddress getRemoteAddress() {
		return remoteAddress;
	}
}
