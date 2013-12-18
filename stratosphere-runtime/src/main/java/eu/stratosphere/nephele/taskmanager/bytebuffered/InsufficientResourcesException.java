/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.bytebuffered;

/**
 * This exception is thrown by the {@link ByteBufferedChannelManager} to indicate that a task cannot be accepted because
 * there are not enough resources available to safely execute it.
 * 
 * @author warneke
 */
public final class InsufficientResourcesException extends Exception {

	/**
	 * The generated serial version UID.
	 */
	private static final long serialVersionUID = -8977049569413215169L;

	/**
	 * Constructs a new insufficient resources exception.
	 * 
	 * @param msg
	 *        the message describing the exception
	 */
	InsufficientResourcesException(final String msg) {
		super(msg);
	}
}
