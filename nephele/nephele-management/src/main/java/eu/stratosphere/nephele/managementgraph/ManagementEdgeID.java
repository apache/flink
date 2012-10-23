/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.managementgraph;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * A class for statistically unique management edge IDs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ManagementEdgeID extends AbstractID {

	/**
	 * Default constructor required by kryo.
	 */
	private ManagementEdgeID() {
	}

	/**
	 * Constructs a new management edge ID.
	 * 
	 * @param lowerPart
	 *        the lower bytes of the ID
	 * @param upperPart
	 *        the higher bytes of the ID
	 */
	private ManagementEdgeID(final long lowerPart, final long upperPart) {
		super(lowerPart, upperPart);
	}

	/**
	 * Constructs a new management edge ID from the given channel ID.
	 * 
	 * @param channelID
	 *        the channel ID to construct the management edge ID from
	 */
	private ManagementEdgeID(final ChannelID channelID) {
		super(channelID);
	}

	/**
	 * Generates a new statistically unique management edge ID.
	 * 
	 * @return a new statistically unique management edge ID
	 */
	public static ManagementEdgeID generate() {

		final long lowerPart = AbstractID.generateRandomBytes();
		final long upperPart = AbstractID.generateRandomBytes();

		return new ManagementEdgeID(lowerPart, upperPart);
	}

	/**
	 * Converts the channel ID into a management edge ID. The new management edge ID will be equal to the channel ID in
	 * the sense that the <code>equals</code> method will return <code>true</code> when both IDs are compared.
	 * 
	 * @return the new management edge ID
	 */
	public static ManagementEdgeID fromChannelID(final ChannelID channelID) {

		return new ManagementEdgeID(channelID);
	}
}
