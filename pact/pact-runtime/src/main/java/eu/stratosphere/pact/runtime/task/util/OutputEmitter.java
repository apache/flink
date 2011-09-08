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

	private final int[] resolveTab_16 = {9,1,7,5,10,9,6,14,2,6,9,6,14,5,14,11,3,10,11,6,13,11,3,2,5,1,6,12,7,3,6,1,10,1,3,15,2,9,14,11,2,11,2,2,9,1,4,1,7,3,10,7,6,1,6,15,13,13,0,2,7,10,6,2,2,11,6,10,3,7,15,1,15,15,2,7,10,9,3,14,11,6,3,9,15,6,6,1,7,2,7,9,9,5,5,5,3,11,5,3,10,14,2,9,5,5,3,14,9,15,13,10,5,2,5,9,15,13,14,1,10,3,6,2,15,9,2,2,15,11,11,15,11,11,3,1,11,6,7,3,15,2,6,14,11,9,10,5,7,2,11,11,11,15,13,7,14,3,15,15,2,3,13,7,6,6,11,3,13,7,5,7,13,9,7,14,11,10,5,3,14,7,15,6,6,1,10,14,9,10,7,5,14,13,1,1,13,15,9,15,5,10,2,9,14,11,14,14,14,15,14,6,14,10,13,10,11,13,8,1,10,10,1,6,7,7,9,14,10,1,7,3,3,15,2,11,9,5,13,15,5,14,6,10,15,15,3,7,3,10,11,2,9,3,7,1};
	private final int[] resolveTab_32 = {11,19,27,30,11,30,14,4,13,6,29,31,3,4,29,20,4,23,14,13,12,23,6,23,12,28,4,24,14,19,10,15,29,5,31,12,20,29,12,22,6,13,29,23,20,19,8,3,28,22,30,15,5,11,29,15,4,28,0,19,4,29,6,6,23,7,13,14,7,6,29,20,31,23,11,28,30,17,21,28,30,22,23,6,15,11,15,1,31,6,14,12,30,30,21,22,21,29,13,11,7,13,22,13,4,30,14,15,23,11,4,20,6,2,31,28,21,23,20,5,19,20,14,13,19,5,29,30,12,21,21,13,5,14,15,31,3,4,4,23,3,14,21,20,31,21,15,9,15,12,28,28,23,14,29,4,5,6,22,31,12,6,7,6,7,7,7,20,15,19,20,28,20,15,20,13,22,7,5,14,30,22,29,28,22,14,5,15,29,13,3,31,28,31,6,12,29,5,5,14,31,22,28,30,7,21,19,26,21,30,7,30,13,18,6,5,7,25,16,14,7,29,15,12,5,30,7,13,5,22,31,23,23,5,21,20,6,12,30,21,22,7,4,7,22,11,31,12,15,19,22,31,21,28,23,21};

	private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
	
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
		this(ShipStrategy.NONE);
	}

	/**
	 * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
	 * 
	 * @param strategy
	 *        The distribution strategy to be used.
	 */
	public OutputEmitter(ShipStrategy strategy)
	{
		this.strategy = strategy;
		this.salt = DEFAULT_SALT;
	}	
		
	public OutputEmitter(ShipStrategy strategy, JobID jobId, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		this(strategy, jobId, DEFAULT_SALT, keyPositions, keyTypes);
	}
	
	public OutputEmitter(ShipStrategy strategy, JobID jobId, byte[] salt , int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		if (strategy == null | jobId == null | salt == null | keyPositions == null | keyTypes == null) { 
			throw new NullPointerException();
		}
		this.strategy = strategy;
		this.salt = salt;
		this.keyPositions = keyPositions;
		this.keyClasses = keyTypes;
		this.jobId = jobId;
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
	
		int code = (hash < 0) ? -hash % 256 : hash % 256;
		this.channels[0] = resolveTab_32[code]; 
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
