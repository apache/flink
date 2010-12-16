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

package eu.stratosphere.nephele.io.channels;

/**
 * A channel setup exception is thrown if a channel could
 * not be set up correctly. Reasons for the incorrect setup can
 * be unknown channel types or lack of resources (e.g. network ports).
 * 
 * @author warneke
 */
public class ChannelSetupException extends Exception {

	/**
	 * Generated serial version UID.
	 */
	private static final long serialVersionUID = 5655978858339124454L;

	/**
	 * Constructs a new channel setup exception.
	 * 
	 * @param msg
	 *        the error message for this exception
	 */
	public ChannelSetupException(String msg) {
		super(msg);
	}

}
