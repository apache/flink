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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * InputChannel is an abstract base class to all different kinds of concrete
 * input channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.
 * 
 * @author warneke
 * @param <T>
 *        The Type of the record that can be transported through the channel.
 */
public abstract class AbstractInputChannel<T extends Record> extends AbstractChannel {

	private InputGate<T> inputGate = null;

	/**
	 * Constructs an input channel with a given input gate associated.
	 * 
	 * @param inputGate
	 * @param channelIndex
	 *        the channel's index at the associated input gate
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	public AbstractInputChannel(InputGate<T> inputGate, int channelIndex, ChannelID channelID,
			CompressionLevel compressionLevel) {
		super(channelIndex, channelID, compressionLevel);
		this.inputGate = inputGate;
	}

	/**
	 * Returns the input gate associated with the input channel.
	 * 
	 * @return the input gate associated with the input channel.
	 */
	public InputGate<T> getInputGate() {
		return this.inputGate;
	}

	/**
	 * Reads a record from the input channel. If currently no record is available the method
	 * returns <code>null</code>. If the channel is closed (i.e. no more records will be received), the method
	 * throws an {@link EOFException}.
	 * 
	 * @return a record that has been transported through the channel or <code>null</code> if currently no record is
	 *         available
	 * @throws IOException
	 *         thrown if the input channel is already closed {@link EOFException} or a transmission error has occurred
	 */
	public abstract T readRecord() throws IOException;

	@Override
	public void read(DataInput in) throws IOException {

		super.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);
	}

	/**
	 * Immediately closes the input channel. The corresponding output channels are
	 * notified if necessary. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the channel to close
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the channel
	 */
	public abstract void close() throws IOException, InterruptedException;

	// TODO: See if type safety can be improved here
	@SuppressWarnings("unchecked")
	public Decompressor getDecompressor(int bufferSize) throws CompressionException {

		if (getCompressionLevel() == CompressionLevel.NO_COMPRESSION)
			throw new CompressionException("CompressionLevel is set to NO_COMPRESSION");

		String configurationKey = null;

		switch (this.getType()) {
		case FILE:
			configurationKey = "channel.file.decompressor";
			break;
		case NETWORK:
			configurationKey = "channel.network.decompressor";
			break;
		}

		if (configurationKey == null)
			throw new CompressionException("Cannot determine configuration key for the channel type " + this.getType());

		String className = GlobalConfiguration.getString(configurationKey, null);
		if (className == null)
			throw new CompressionException("Configuration does not contain an entry for key " + configurationKey);

		Class<? extends Decompressor> decompressionClass = null;

		try {
			decompressionClass = (Class<? extends Decompressor>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new CompressionException("Cannot find decompressor class: " + StringUtils.stringifyException(e));
		}

		Constructor<? extends Decompressor> constructor = null;

		try {
			constructor = decompressionClass.getConstructor(int.class, CompressionLevel.class);
		} catch (SecurityException e) {
			throw new CompressionException(StringUtils.stringifyException(e));
		} catch (NoSuchMethodException e) {
			throw new CompressionException("Cannot find matching constructor for decompression class: "
				+ StringUtils.stringifyException(e));
		}

		Decompressor decompressor = null;

		try {
			decompressor = constructor.newInstance(bufferSize, getCompressionLevel());
		} catch (Exception e) {
			throw new CompressionException(StringUtils.stringifyException(e));
		}

		return decompressor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {
		return this.inputGate.getJobID();
	}
}
