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

import java.io.IOException;
import java.lang.reflect.Constructor;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * OutputChannel is an abstract base class to all different kinds of concrete
 * output channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.
 * 
 * @author warneke
 * @param <T>
 *        The Type of the record that can be transported through the channel.
 */
public abstract class AbstractOutputChannel<T extends Record> extends AbstractChannel {

	private final OutputGate<T> outputGate;

	/**
	 * Creates a new output channel object.
	 * 
	 * @param outputGate
	 *        the output gate this channel is connected to
	 * @param channelIndex
	 *        the index of the channel in the output gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	public AbstractOutputChannel(final OutputGate<T> outputGate, final int channelIndex, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {
		super(channelIndex, channelID, connectedChannelID, compressionLevel);
		this.outputGate = outputGate;
	}

	/**
	 * Returns the output gate this channel is connected to.
	 * 
	 * @return the output gate this channel is connected to
	 */
	public OutputGate<T> getOutputGate() {
		return this.outputGate;
	}

	/**
	 * Writes a record to the channel. The operation may block until the record
	 * is completely written to the channel.
	 * 
	 * @param record
	 *        the record to be written to the channel
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the record
	 */
	public abstract void writeRecord(T record) throws IOException, InterruptedException;

	/**
	 * Requests the output channel to close. After calling this method no more records can be written
	 * to the channel. The channel is finally closed when all remaining data that may exist in internal buffers
	 * are written to the channel.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while requesting the close operation
	 * @throws IOException
	 *         thrown if an I/O error occurs while requesting the close operation
	 */
	public abstract void requestClose() throws IOException, InterruptedException;

	// TODO: See if type safety can be improved here
	@SuppressWarnings("unchecked")
	public Compressor getCompressor(int bufferSize) throws CompressionException {

		if (getCompressionLevel() == CompressionLevel.NO_COMPRESSION)
			throw new CompressionException("CompressionLevel is set to NO_COMPRESSION");

		String configurationKey = null;

		switch (this.getType()) {
		case FILE:
			configurationKey = "channel.file.compressor";
			break;
		case NETWORK:
			configurationKey = "channel.network.compressor";
			break;
		}

		if (configurationKey == null)
			throw new CompressionException("Cannot determine configuration key for the channel type " + this.getType());

		String className = GlobalConfiguration.getString(configurationKey, null);
		if (className == null)
			throw new CompressionException("Configuration does not contain an entry for key " + configurationKey);

		Class<? extends Compressor> compressionClass = null;

		try {
			compressionClass = (Class<? extends Compressor>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new CompressionException("Cannot find compressor class: " + StringUtils.stringifyException(e));
		}

		Constructor<? extends Compressor> constructor = null;

		try {
			constructor = compressionClass.getConstructor(int.class, CompressionLevel.class);
		} catch (SecurityException e) {
			throw new CompressionException(StringUtils.stringifyException(e));
		} catch (NoSuchMethodException e) {
			throw new CompressionException("Cannot find matching constructor for compression class: "
				+ StringUtils.stringifyException(e));
		}

		Compressor compressor = null;

		try {
			compressor = constructor.newInstance(bufferSize, getCompressionLevel());
		} catch (Exception e) {
			throw new CompressionException(StringUtils.stringifyException(e));
		}

		return compressor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {
		return false;
	}

	public abstract void flush() throws IOException, InterruptedException;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {
		return this.outputGate.getJobID();
	}

	/**
	 * Returns <code>true</code> if this channel is connected to an output gate which operates in broadcast mode,
	 * <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if the connected output gate operates in broadcase mode, <code>false</code> otherwise
	 */
	public boolean isBroadcastChannel() {

		return this.outputGate.isBroadcast();
	}
}
