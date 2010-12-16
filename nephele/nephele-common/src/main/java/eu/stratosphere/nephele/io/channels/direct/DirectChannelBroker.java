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

package eu.stratosphere.nephele.io.channels.direct;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.types.Record;

/**
 * The direct channel broker helps an unconnected {@link AbstractDirectOutputChannel} to find
 * its corresponding {@link AbstractDirectInputChannel} and vice-versa.
 * 
 * @author warneke
 */
public interface DirectChannelBroker {

	/**
	 * The direct channel broker tries to find an {@link AbstractDirectInputChannel} with
	 * a matching ID in its database and returns it to the requesting {@link AbstractDirectOutputChannel}.
	 * 
	 * @param id
	 *        the ID of the channel to find
	 * @return the {@link AbstractDirectInputChannel} with the matching ID or <code>null</code> if no matching channel
	 *         could be found
	 */
	AbstractDirectInputChannel<? extends Record> getDirectInputChannelByID(ChannelID id);

	/**
	 * The direct channel broker tries to find an {@link AbstractDirectOutputChannel} with
	 * a matching ID in its database and returns it to the requesting {@link AbstractDirectInputChannel}.
	 * 
	 * @param id
	 *        the ID of the channel to find
	 * @return the {@link AbstractOutputChannel} with the matching ID or <code>null</code> if no matching channel could
	 *         be found
	 */
	AbstractDirectOutputChannel<? extends Record> getDirectOutputChannelByID(ChannelID id);
}
