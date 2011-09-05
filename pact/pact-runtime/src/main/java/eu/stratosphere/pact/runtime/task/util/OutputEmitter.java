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

package eu.stratosphere.pact.runtime.task.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * @author Erik Nijkamp
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public class OutputEmitter implements ChannelSelector<PactRecord>
{
	/**
	 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
	 * or re-partitioning by range.
	 */
	public enum ShipStrategy {
		FORWARD,
		BROADCAST,
		PARTITION_HASH,
		PARTITION_RANGE,
		PARTITION_LOCAL_HASH,
		PARTITION_LOCAL_RANGE,
		SFR,
		NONE
	}

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------
	
	private ShipStrategy strategy;				// the shipping strategy used by this output emitter
	
	private int[] channels;						// the reused array defining target channels
	
	private int nextChannelToSendTo = 0;		// counter to go over channels round robin
	
	private Class<? extends Key>[] keyClasses;
	
	private int[] keyPositions;
	
	private final byte[] salt;					// the salt used to randomize the hash values
	
	private JobID jobId;						// the job ID is necessary to obtain the class loader

	private PartitionFunction partitionFunction;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new channel selector that distributes data round robin.
	 */
	public OutputEmitter()
	{
		this(ShipStrategy.NONE, null, null, null);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy
	 *        The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategy strategy)
	{
		this(strategy, null, null, null);
	}	
		
	public OutputEmitter(ShipStrategy strategy, JobID jobId, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		this(strategy, jobId, new byte[] { 17, 31, 47, 51, 83, 1 }, keyPositions, keyTypes);
	}
	
	public OutputEmitter(ShipStrategy strategy, JobID jobId, byte[] salt , int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		this.strategy = strategy;
		this.salt = salt;
		this.keyPositions = keyPositions;
		this.keyClasses = keyTypes;
	}

	// ------------------------------------------------------------------------
	// Channel Selection
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.ChannelSelector#selectChannels(java.lang.Object, int)
	 */
	@Override
	public final int[] selectChannels(PactRecord record, int numberOfChannels)
	{
		switch (strategy) {
		case BROADCAST:
			return broadcast(numberOfChannels);
		case PARTITION_HASH:
		case PARTITION_LOCAL_HASH:
			return hashPartitionDefault(record, numberOfChannels);
		case FORWARD:
			return robin(numberOfChannels);
		case PARTITION_RANGE:
			return partition_range(record, numberOfChannels);
		default:
			throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
		}
	}
	
	/**
	 * Set the partition function that is used for range partitioning
	 * @param func
	 */
	public void setPartitionFunction(PartitionFunction func) {
		this.partitionFunction = func;
	}

	private int[] partition_range(PactRecord record, int numberOfChannels) {
		return partitionFunction.selectChannels(record, numberOfChannels);
	}
	
	// --------------------------------------------------------------------------------------------

	private final int[] robin(int numberOfChannels)
	{
		if (this.channels == null || this.channels.length != 1) {
			this.channels = new int[1];
		}
		
		int nextChannel = nextChannelToSendTo + 1;
		nextChannel = nextChannel < numberOfChannels ? nextChannel : 0;
		
		this.nextChannelToSendTo = nextChannel;
		this.channels[0] = nextChannel;
		return this.channels;
	}

	private final int[] broadcast(int numberOfChannels)
	{
		if (channels == null || channels.length != numberOfChannels) {
			channels = new int[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++)
				channels[i] = i;
		}

		return channels;
	}

	private final int[] hashPartitionDefault(PactRecord record, int numberOfChannels)
	{
		if (channels == null || channels.length != 1) {
			channels = new int[1];
		}
		
		int hash = 0;
		for (int i = 0; i < this.keyPositions.length; i++) {
			final Key k = record.getField(this.keyPositions[i], this.keyClasses[i]);
			hash ^= (1315423911 ^ ((1315423911 << 5) + k.hashCode() + (1315423911 >> 2)));
		}
		
		for (int i = 0; i < salt.length; i++) {
			hash ^= ((hash << 5) + salt[i] + (hash >> 2));
		}
	
		this.channels[0] = (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
		return this.channels;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		// strategy
		this.strategy = ShipStrategy.valueOf(in.readUTF());
		
		// check whether further parameters come
		final boolean keyParameterized = in.readBoolean();
		
		if (keyParameterized) {
			// read the jobID to find the classloader
			this.jobId = new JobID();
			this.jobId.read(in);			
			final ClassLoader loader = LibraryCacheManager.getClassLoader(this.jobId);
		
			// read the number of keys and key positions
			int numKeys = in.readInt();
			this.keyPositions = new int[numKeys];
			for (int i = 0; i < numKeys; i++) {
				this.keyPositions[i] = in.readInt();
			}
			
			// read the key types
			@SuppressWarnings("unchecked")
			Class<? extends Key>[] classes = (Class<? extends Key>[]) new Class[numKeys];
			try {
				for (int i = 0; i < numKeys; i++) {
					String className = in.readUTF();
					classes[i] = Class.forName(className, true, loader).asSubclass(Key.class);
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Output Emmitter is unable to load the classes that describe the key types: "
					+ e.getMessage(), e); 
			}
			this.keyClasses = classes;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(strategy.name());
		
		if (this.keyClasses != null) {
			// write additional info
			out.writeBoolean(true);
			this.jobId.write(out);
			
			// write number of keys, key positions and key types
			out.writeInt(this.keyClasses.length);
			for (int i = 0; i < this.keyPositions.length; i++) {
				out.writeInt(this.keyPositions[i]);
			}
			for (int i = 0; i < this.keyClasses.length; i++) {
				out.writeUTF(this.keyClasses[i].getName());
			}
		}
		else {
			out.writeBoolean(false);
		}
	}
}
