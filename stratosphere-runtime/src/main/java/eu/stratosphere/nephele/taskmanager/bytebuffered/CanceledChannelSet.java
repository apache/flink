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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * This channel set stores the ID's of all channels whose tasks have been canceled recently. The set is cleaned up by
 * periodically calling the method <code>cleanup</code>.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class CanceledChannelSet implements Set<ChannelID> {

	/**
	 * The period of time the entries must at least remain in the map.
	 */
	private final static long CLEANUP_INTERVAL = 30000; // 30 sec.

	/**
	 * The map which stores the ID's of the channels whose tasks have been canceled.
	 */
	private final Map<ChannelID, Long> canceledChannels = new HashMap<ChannelID, Long>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean add(ChannelID arg0) {

		final long now = System.currentTimeMillis();

		synchronized (this.canceledChannels) {
			if (this.canceledChannels.put(arg0, Long.valueOf(now)) == null) {
				return true;
			}
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(Collection<? extends ChannelID> arg0) {

		final Long now = Long.valueOf(System.currentTimeMillis());
		final Iterator<? extends ChannelID> it = arg0.iterator();
		boolean retVal = false;

		synchronized (this.canceledChannels) {

			while (it.hasNext()) {

				if (this.canceledChannels.put(it.next(), now) == null) {
					retVal = true;
				}
			}
		}

		return retVal;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {

		synchronized (this.canceledChannels) {
			this.canceledChannels.clear();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(Object arg0) {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.containsKey(arg0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsAll(Collection<?> arg0) {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.keySet().containsAll(arg0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.isEmpty();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<ChannelID> iterator() {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.keySet().iterator();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(Object arg0) {

		synchronized (this.canceledChannels) {
			if (this.canceledChannels.remove(arg0) == null) {
				return false;
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAll(Collection<?> arg0) {

		final Iterator<?> it = arg0.iterator();
		boolean retVal = false;

		synchronized (this.canceledChannels) {

			while (it.hasNext()) {
				if (this.canceledChannels.remove(it.next()) != null) {
					retVal = true;
				}
			}
		}

		return retVal;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean retainAll(Collection<?> arg0) {

		throw new RuntimeException("Method not implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.size();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object[] toArray() {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.keySet().toArray();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> T[] toArray(T[] arg0) {

		synchronized (this.canceledChannels) {
			return this.canceledChannels.keySet().toArray(arg0);
		}
	}

	/**
	 * Removes all entries from the set which have been added longer than <code>CLEANUP_INTERVAL</code> milliseconds
	 * ago.
	 */
	public void cleanup() {

		final long now = System.currentTimeMillis();

		synchronized (this.canceledChannels) {

			final Iterator<Map.Entry<ChannelID, Long>> it = this.canceledChannels.entrySet().iterator();
			while (it.hasNext()) {

				final Map.Entry<ChannelID, Long> entry = it.next();
				if ((entry.getValue().longValue() + CLEANUP_INTERVAL) < now) {
					it.remove();
				}
			}
		}
	}
}
