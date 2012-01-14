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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.types.Record;

/**
 * This channel set stores the IDs of all channels that have been recently removed. The set can be cleaned up by
 * periodically calling the method <code>cleanup</code>.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class RecentlyRemovedChannelIDSet {

	/**
	 * The period of time the entries must at least remain in the map.
	 */
	private final static long CLEANUP_INTERVAL = 30000; // 30 sec.

	/**
	 * The map which stores the ID's of the channels whose tasks have been canceled.
	 */
	private final Map<ChannelID, Long> recentlyRemovedChannels = new HashMap<ChannelID, Long>();

	/**
	 * Checks whether the given channel ID is stored within this set.
	 * 
	 * @param channelID
	 *        the channel ID to check for
	 * @return <code>true</code> if the given channel ID was found in the set, <code>false</code> otherwise
	 */
	public boolean contains(final ChannelID channelID) {

		synchronized (this.recentlyRemovedChannels) {

			return this.recentlyRemovedChannels.containsKey(channelID);
		}
	}

	/**
	 * Removes all entries from the set which have been added longer than <code>CLEANUP_INTERVAL</code> milliseconds
	 * ago.
	 */
	public void cleanup() {

		final long now = System.currentTimeMillis();

		synchronized (this.recentlyRemovedChannels) {

			final Iterator<Map.Entry<ChannelID, Long>> it = this.recentlyRemovedChannels.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<ChannelID, Long> entry = it.next();
				if ((entry.getValue().longValue() + CLEANUP_INTERVAL) < now) {
					it.remove();
				}
			}
		}
	}

	/**
	 * Adds the IDs of all the channels that are attached to the given environment to this set.
	 * 
	 * @param environment
	 *        the environment whose IDs shall be added to this set
	 */
	public void add(final Environment environment) {

		final Long now = Long.valueOf(System.currentTimeMillis());

		synchronized (this.recentlyRemovedChannels) {

			final int numberOfOutputGates = environment.getNumberOfOutputGates();

			for (int i = 0; i < numberOfOutputGates; ++i) {

				final OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
				final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
				for (int j = 0; j < numberOfOutputChannels; ++j) {

					this.recentlyRemovedChannels.put(outputGate.getOutputChannel(j).getID(), now);
				}
			}

			final int numberOfInputGates = environment.getNumberOfInputGates();

			for (int i = 0; i < numberOfInputGates; ++i) {

				final InputGate<? extends Record> inputGate = environment.getInputGate(i);
				final int numberOfInputChannels = inputGate.getNumberOfInputChannels();
				for (int j = 0; j < numberOfInputChannels; ++j) {

					this.recentlyRemovedChannels.put(inputGate.getInputChannel(j).getID(), now);
				}
			}
		}
	}
}
