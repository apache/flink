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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.FileUtils;

final class LocalChannelWithAccessInfo implements ChannelWithAccessInfo {

	/**
	 * The logging object.
	 */
	private static final Log LOG = LogFactory.getLog(LocalChannelWithAccessInfo.class);

	private final File file;

	private final FileChannel channel;

	private final AtomicLong reservedWritePosition;

	private final AtomicInteger referenceCounter;

	private final AtomicBoolean deleteOnClose;

	LocalChannelWithAccessInfo(final File file, final boolean deleteOnClose) throws IOException {

		this.file = file;
		this.channel = new RandomAccessFile(file, "rw").getChannel();
		this.reservedWritePosition = new AtomicLong(0L);
		this.referenceCounter = new AtomicInteger(0);
		this.deleteOnClose = new AtomicBoolean(deleteOnClose);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileChannel getChannel() {

		return this.channel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileChannel getAndIncrementReferences() {

		if (incrementReferences()) {
			return this.channel;
		} else {
			return null;
		}
	}

	@Override
	public ChannelWithPosition reserveWriteSpaceAndIncrementReferences(final int spaceToReserve) {

		if (incrementReferences()) {
			return new ChannelWithPosition(this.channel, this.reservedWritePosition.getAndAdd(spaceToReserve));
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int decrementReferences() {

		int current = this.referenceCounter.get();
		while (true) {
			if (current <= 0) {
				// this is actually an error case, because the channel was deleted before
				throw new IllegalStateException("The references to the file were already at zero.");
			}

			if (current == 1) {
				// this call decrements to zero, so mark it as deleted
				if (this.referenceCounter.compareAndSet(current, Integer.MIN_VALUE)) {
					current = 0;
					break;
				}
			} else if (this.referenceCounter.compareAndSet(current, current - 1)) {
				current = current - 1;
				break;
			}
			current = this.referenceCounter.get();
		}

		if (current > 0) {
			return current;
		} else if (current == 0) {
			// delete the channel
			this.referenceCounter.set(Integer.MIN_VALUE);
			this.reservedWritePosition.set(Long.MIN_VALUE);
			try {
				this.channel.close();
			} catch (IOException ioex) {
				if (LOG.isErrorEnabled())
					LOG.error("Error while closing spill file for file buffers: " + ioex.getMessage(), ioex);
			}
			if (this.deleteOnClose.get()) {
				FileUtils.deleteSilently(this.file);
			}
			return current;
		} else {
			throw new IllegalStateException("The references to the file were already at zero.");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean incrementReferences() {

		int current = this.referenceCounter.get();
		while (true) {
			// check whether it was disposed in the meantime
			if (current < 0) {
				return false;
			}
			// atomically check and increment
			if (this.referenceCounter.compareAndSet(current, current + 1)) {
				return true;
			}
			current = this.referenceCounter.get();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void disposeSilently() {

		this.referenceCounter.set(Integer.MIN_VALUE);
		this.reservedWritePosition.set(Long.MIN_VALUE);

		if (this.channel.isOpen()) {
			try {
				this.channel.close();
			} catch (Throwable t) {
			}
		}

		if (this.deleteOnClose.get()) {
			FileUtils.deleteSilently(this.file);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateDeleteOnCloseFlag(final boolean deleteOnClose) {

		this.deleteOnClose.compareAndSet(true, deleteOnClose);
	}
}
