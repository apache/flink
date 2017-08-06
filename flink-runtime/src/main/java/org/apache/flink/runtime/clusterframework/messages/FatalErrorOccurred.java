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

package org.apache.flink.runtime.clusterframework.messages;

import org.apache.flink.util.SerializedThrowable;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Message sent to the Flink's resource manager in case of a fatal error that
 * cannot be recovered in the running process.
 * 
 * When master high-availability is enabled, this message should fail the resource
 * manager (for example process kill) such that it gets recovered (restarted or
 * another process takes over).
 */
public class FatalErrorOccurred implements Serializable {

	private static final long serialVersionUID = -2246792138413563536L;
	
	private final String message;
	
	private final SerializedThrowable error;
	
	public FatalErrorOccurred(String message, Throwable error) {
		this.message = requireNonNull(message);
		this.error = new SerializedThrowable(requireNonNull(error));
	}
	
	// ------------------------------------------------------------------------
	
	public String message() {
		return message;
	}
	
	public Throwable error() {
		return error.deserializeError(getClass().getClassLoader());
	}

	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "FatalErrorOccurred { message: '" + message + "', error: '"
			+ error.getMessage() + "' }";
	}
}
