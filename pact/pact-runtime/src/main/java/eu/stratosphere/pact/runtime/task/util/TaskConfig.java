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
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.runtime.task.chaining.ChainedTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * Configuration class which stores all relevant parameters required to set up the Pact tasks.
 * 
 * @author Erik Nijkamp
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class TaskConfig
{
	/**
	 * Enumeration of all available local strategies for Pact tasks. 
	 */
	public enum LocalStrategy {
		// both inputs are sorted and merged
		SORT_BOTH_MERGE,
		// the first input is sorted and merged with the (already sorted) second input
		SORT_FIRST_MERGE,
		// the second input is sorted and merged with the (already sorted) first input
		SORT_SECOND_MERGE,
		// both (already sorted) inputs are merged
		MERGE,
		// input is sorted, within a key values are crossed in a nested loop fashion
		SORT_SELF_NESTEDLOOP,
		// already grouped input, within a key values are crossed in a nested loop fashion
		SELF_NESTEDLOOP,
		// the input is sorted
		SORT,
		// the input is sorted, during sorting a combiner is applied
		COMBININGSORT,
		// the first input is build side, the second side is probe side of a hybrid hash table
		HYBRIDHASH_FIRST,
		// the second input is build side, the first side is probe side of a hybrid hash table
		HYBRIDHASH_SECOND,
		// the first input is build side, the second side is probe side of an in-memory hash table
		MMHASH_FIRST,
		// the second input is build side, the first side is probe side of an in-memory hash table
		MMHASH_SECOND,
		// the second input is inner loop, the first input is outer loop and block-wise processed
		NESTEDLOOP_BLOCKED_OUTER_FIRST,
		// the first input is inner loop, the second input is outer loop and block-wise processed
		NESTEDLOOP_BLOCKED_OUTER_SECOND,
		// the second input is inner loop, the first input is outer loop and stream-processed
		NESTEDLOOP_STREAMED_OUTER_FIRST,
		// the first input is inner loop, the second input is outer loop and stream-processed
		NESTEDLOOP_STREAMED_OUTER_SECOND,
		// no special local strategy is applied
		NONE
	}
	
	// --------------------------------------------------------------------------------------------

	private static final String STUB_CLASS = "pact.stub.class";

	private static final String STUB_PARAM_PREFIX = "pact.stub.param.";

	private static final String INPUT_SHIP_STRATEGY = "pact.input.ship.strategy";
	
	private static final String INPUT_SHIP_NUM_KEYS = "pact.input.numkeys";
	
	private static final String INPUT_SHIP_KEY_POS_PREFIX = "pact.input.keypos.";
	
	private static final String INPUT_SHIP_KEY_CLASS_PREFIX = "pact.input.keyclass.";

	private static final String OUTPUT_SHIP_STRATEGY_PREFIX = "pact.output.shipstrategy.";
	
	private static final String OUTPUT_SHIP_NUM_KEYS_PREFIX = "pact.output.numkeys.";
	
	private static final String OUTPUT_SHIP_KEY_POS_PREFIX = "pact.output.keypos.";
	
	private static final String OUTPUT_SHIP_KEY_CLASS_PREFIX = "pact.output.keyclass.";

	private static final String LOCAL_STRATEGY = "pact.local.strategy";

	private static final String NUM_INPUTS = "pact.inputs.number";

	private static final String NUM_OUTPUTS = "pact.outputs.number";

	private static final String SIZE_MEMORY = "pact.memory.size";

	private static final String NUM_FILEHANDLES = "pact.filehandles.num";
	
	private static final String SORT_SPILLING_THRESHOLD = "pact.sort.spillthreshold";
	
	private static final String CHAINING_NUM_STUBS = "pact.chaining.num";
	
	private static final String CHAINING_TASKCONFIG_PREFIX = "pact.chaining.taskconfig.";
	
	private static final String CHAINING_TASK_PREFIX = "pact.chaining.task.";
	
	private static final String CHAINING_TASKNAME_PREFIX = "pact.chaining.taskname.";

	// --------------------------------------------------------------------------------------------
	
	protected final Configuration config;			// the actual configuration holding the values

	
	public TaskConfig(Configuration config)
	{
		this.config = config;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                User code class Access
	// --------------------------------------------------------------------------------------------

	public void setStubClass(Class<?> stubClass) {
		config.setString(STUB_CLASS, stubClass.getName());
	}

	public <T> Class<? extends T> getStubClass(Class<T> stubClass, ClassLoader cl)
	throws ClassNotFoundException, ClassCastException
	{
		String stubClassName = config.getString(STUB_CLASS, null);
		if (stubClassName == null) {
			throw new IllegalStateException("stub class missing");
		}
		return Class.forName(stubClassName, true, cl).asSubclass(stubClass);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                User Code Parameters
	// --------------------------------------------------------------------------------------------

	public void setStubParameters(Configuration parameters)
	{
		this.config.addAll(parameters, STUB_PARAM_PREFIX);
	}

	public Configuration getStubParameters()
	{
		return new DelegatingConfiguration(this.config, STUB_PARAM_PREFIX);
	}
	
	public void setStubParameter(String key, String value)
	{
		this.config.setString(STUB_PARAM_PREFIX + key, value);
	}

	public String getStubParameter(String key, String defaultValue)
	{
		return this.config.getString(STUB_PARAM_PREFIX + key, defaultValue);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Input Shipping
	// --------------------------------------------------------------------------------------------

	public void addInputShipStrategy(ShipStrategy strategy) {
		int inputCnt = config.getInteger(NUM_INPUTS, 0);
		config.setString(INPUT_SHIP_STRATEGY + (inputCnt++), strategy.name());
		config.setInteger(NUM_INPUTS, inputCnt);
	}
	
	public int getNumInputs() {
		return config.getInteger(NUM_INPUTS, -1);
	}

	public ShipStrategy getInputShipStrategy(int inputId) {
		int inputCnt = config.getInteger(NUM_INPUTS, -1);
		if (!(inputId < inputCnt)) {
			return null;
		}
		return ShipStrategy.valueOf(config.getString(INPUT_SHIP_STRATEGY + inputId, ""));
	}
	
	// --------------------------------------------------------------------------------------------
	//                                 Local Strategies
	// --------------------------------------------------------------------------------------------

	public void setLocalStrategy(LocalStrategy strategy) {
		this.config.setString(LOCAL_STRATEGY, strategy.name());
	}

	public LocalStrategy getLocalStrategy() {
		String lsName = this.config.getString(LOCAL_STRATEGY, null);
		return lsName != null ? LocalStrategy.valueOf(lsName) : LocalStrategy.NONE;
	}
	
	public void setLocalStrategyKeyTypes(Class<? extends Key>[] keyTypes)
	{
		int numKeysYet = this.config.getInteger(INPUT_SHIP_NUM_KEYS, -1);
		if (numKeysYet == -1) {
			this.config.setInteger(INPUT_SHIP_NUM_KEYS, keyTypes.length);
		}
		else if (keyTypes.length != numKeysYet) {
			throw new IllegalArgumentException("The number of key classes does not match the number of keys set by a previous parameter.");
		}

		for (int i = 0; i < keyTypes.length; i++) {
			this.config.setString(INPUT_SHIP_KEY_CLASS_PREFIX + i, keyTypes[i].getName());
		}
	}
	
	public void setLocalStrategyKeyTypes(int inputNum, int[] keyPositions)
	{			
		int numKeysYet = this.config.getInteger(INPUT_SHIP_NUM_KEYS, -1);
		if (numKeysYet == -1) {
			this.config.setInteger(INPUT_SHIP_NUM_KEYS, keyPositions.length);
		}
		else if (keyPositions.length != numKeysYet) {
			throw new IllegalArgumentException("The number of positions does not match the number of keys set by a previous parameter.");
		}
		
		for (int i = 0; i < keyPositions.length; i++) {
			this.config.setInteger(INPUT_SHIP_KEY_POS_PREFIX + inputNum + '.' + i, keyPositions[i]);
		}
	}
	
	public int[] getLocalStrategyKeyPositions(int inputNum)
	{		
		final int numKeys = this.config.getInteger(INPUT_SHIP_NUM_KEYS , -1);
		if (numKeys <= 0) {
			return null;
		}
		
		final int[] keyPos = new int[numKeys];
		for (int i = 0; i < numKeys; i++) {
			int p = this.config.getInteger(INPUT_SHIP_KEY_POS_PREFIX + inputNum + '.' + i, -1);
			if (p >= 0) {
				keyPos[i] = p;
			} else {
				throw new IllegalStateException("Config is invalid - contained number of keys, but no positions for keys."); 
			}
		}
		return keyPos;
	}
	
	public Class<? extends Key>[] getLocalStrategyKeyClasses(ClassLoader cl)
	throws ClassNotFoundException, ClassCastException
	{
		final int numKeys = this.config.getInteger(INPUT_SHIP_NUM_KEYS, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = (Class<? extends Key>[]) new Class[numKeys];
		for (int i = 0; i < numKeys; i++) {
			String name = this.config.getString(INPUT_SHIP_KEY_CLASS_PREFIX + i, null);
			if (name != null) {
				keyTypes[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new IllegalStateException("Config is invalid - contained number of keys, but no types for keys."); 
			}
		}
		return keyTypes;
	}
	
	// --------------------------------------------------------------------------------------------
	//                          Parameters for the output shipping
	// --------------------------------------------------------------------------------------------

	public void addOutputShipStrategy(ShipStrategy strategy)
	{
		int outputCnt = config.getInteger(NUM_OUTPUTS, 0);		
		this.config.setString(OUTPUT_SHIP_STRATEGY_PREFIX + outputCnt, strategy.name());
		outputCnt++;
		this.config.setInteger(NUM_OUTPUTS, outputCnt);
	}
	
	public void addOutputShipStrategy(ShipStrategy strategy, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		int outputCnt = config.getInteger(NUM_OUTPUTS, 0);
		
		this.config.setString(OUTPUT_SHIP_STRATEGY_PREFIX + outputCnt, strategy.name());		
		this.config.setInteger(OUTPUT_SHIP_NUM_KEYS_PREFIX + outputCnt, keyPositions.length);
		for (int i = 0; i < keyPositions.length; i++) {
			this.config.setInteger(OUTPUT_SHIP_KEY_POS_PREFIX + outputCnt + '.' + i, keyPositions[i]);
			this.config.setString(OUTPUT_SHIP_KEY_CLASS_PREFIX + outputCnt + '.' + i, keyTypes[i].getName());
		}
		outputCnt++;
		this.config.setInteger(NUM_OUTPUTS, outputCnt);
	}
	
	public int getNumOutputs() {
		return config.getInteger(NUM_OUTPUTS, -1);
	}

	public ShipStrategy getOutputShipStrategy(int outputId)
	{
		int outputCnt = this.config.getInteger(NUM_OUTPUTS, -1);
		if (!(outputId < outputCnt)) {
			return null;
		}
		return ShipStrategy.valueOf(this.config.getString(OUTPUT_SHIP_STRATEGY_PREFIX + outputId, ""));
	}
	
	public int[] getOutputShipKeyPositions(int outputId)
	{
		final int outputCnt = this.config.getInteger(NUM_OUTPUTS, -1);
		if (!(outputId < outputCnt)) {
			return null;
		}
		
		final int numKeys = this.config.getInteger(OUTPUT_SHIP_NUM_KEYS_PREFIX + outputId, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		final int[] keyPos = new int[numKeys];
		for (int i = 0; i < numKeys; i++) {
			int p = this.config.getInteger(OUTPUT_SHIP_KEY_POS_PREFIX + outputId + '.' + i, -1);
			if (p >= 0) {
				keyPos[i] = p;
			} else {
				throw new IllegalStateException("Config is invalid - contained number of keys, but no positions for keys."); 
			}
		}
		return keyPos;
	}
	
	public Class<? extends Key>[] getOutputShipKeyTypes(int outputId, ClassLoader cl)
	throws ClassNotFoundException, ClassCastException
	{
		final int outputCnt = this.config.getInteger(NUM_OUTPUTS, -1);
		if (!(outputId < outputCnt)) {
			return null;
		}
		
		final int numKeys = this.config.getInteger(OUTPUT_SHIP_NUM_KEYS_PREFIX + outputId, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = (Class<? extends Key>[]) new Class[numKeys];
		for (int i = 0; i < numKeys; i++) {
			String name = this.config.getString(OUTPUT_SHIP_KEY_CLASS_PREFIX + outputId + '.' + i, null);
			if (name != null) {
				keyTypes[i] = Class.forName(name, true, cl).asSubclass(Key.class);
			} else {
				throw new IllegalStateException("Config is invalid - contained number of keys, but no types for keys."); 
			}
		}
		return keyTypes;
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the amount of memory dedicated to the task's input preparation (sorting / hashing).
	 * 
	 * @param memSize The memory size in bytes.
	 */
	public void setMemorySize(long memorySize) {
		config.setLong(SIZE_MEMORY, memorySize);
	}

	/**
	 * Sets the maximum number of open files.
	 * 
	 * @param numFileHandles Maximum number of open files.
	 */
	public void setNumFilehandles(int numFileHandles) {
		if (numFileHandles < 2) {
			throw new IllegalArgumentException();
		}
		
		config.setInteger(NUM_FILEHANDLES, numFileHandles);
	}
	
	/**
	 * Sets the threshold that triggers spilling to disk of intermediate sorted results. This value defines the
	 * fraction of the buffers that must be full before the spilling is triggered.
	 * 
	 * @param threshold The value for the threshold.
	 */
	public void setSortSpillingTreshold(float threshold) {
		if (threshold < 0.0f || threshold > 1.0f) {
			throw new IllegalArgumentException();
		}
		
		config.setFloat(SORT_SPILLING_THRESHOLD, threshold);
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the amount of memory dedicated to the task's input preparation (sorting / hashing).
	 * Returns <tt>-1</tt> if the value is not specified.
	 * 
	 * @return The memory size in bytes.
	 */
	public long getMemorySize() {
		return config.getLong(SIZE_MEMORY, -1);
	}

	/**
	 * Gets the maximum number of open files. Returns <tt>-1</tt>, if the value has not been set.
	 * 
	 * @return Maximum number of open files.
	 */
	public int getNumFilehandles() {
		return config.getInteger(NUM_FILEHANDLES, -1);
	}
	
	/**
	 * Gets the threshold that triggers spilling to disk of intermediate sorted results. This value defines the
	 * fraction of the buffers that must be full before the spilling is triggered.
	 * <p>
	 * If the value is not set, this method returns a default value if <code>0.7f</code>.
	 * 
	 * @return The threshold that triggers spilling to disk of sorted intermediate results.
	 */
	public float getSortSpillingTreshold() {
		return config.getFloat(SORT_SPILLING_THRESHOLD, 0.7f);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Parameters for Stub Chaining
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfChainedStubs() {
		return this.config.getInteger(CHAINING_NUM_STUBS, 0);
	}
	
	public void addChainedTask(Class<? extends ChainedTask> chainedTaskClass, TaskConfig conf, String taskName)
	{
		int numChainedYet = this.config.getInteger(CHAINING_NUM_STUBS, 0);
		
		this.config.setString(CHAINING_TASK_PREFIX + numChainedYet, chainedTaskClass.getName());
		this.config.addAll(conf.config, CHAINING_TASKCONFIG_PREFIX + numChainedYet + '.');
		this.config.setString(CHAINING_TASKNAME_PREFIX + numChainedYet, taskName);
		
		this.config.setInteger(CHAINING_NUM_STUBS, ++numChainedYet);
	}
	
	public TaskConfig getChainedStubConfig(int chainPos)
	{
		return new TaskConfig(new DelegatingConfiguration(this.config, CHAINING_TASKCONFIG_PREFIX + chainPos + '.'));
	}

	public Class<? extends ChainedTask> getChainedTask(int chainPos)
	throws ClassNotFoundException, ClassCastException
	{
		String className = this.config.getString(CHAINING_TASK_PREFIX + chainPos, null);
		if (className == null)
			throw new IllegalStateException("Chained Task Class missing");
		
		return Class.forName(className).asSubclass(ChainedTask.class);
	}
	
	public String getChainedTaskName(int chainPos)
	{
		return this.config.getString(CHAINING_TASKNAME_PREFIX + chainPos, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//                              Utility class for nested Configurations
	// --------------------------------------------------------------------------------------------
	
	public static final class DelegatingConfiguration extends Configuration
	{
		private final Configuration backingConfig;		// the configuration actually storing the data
		
		private String prefix;							// the prefix key by which keys for this config are marked
		
		// --------------------------------------------------------------------------------------------
		
		/**
		 * Default constructor for serialization. Creates an empty delegating configuration.
		 */
		public DelegatingConfiguration()
		{
			this.backingConfig = new Configuration();
		}
		
		/**
		 * Creates a new delegating configuration which stores its key/value pairs in the given
		 * configuration using the specifies key prefix.
		 * 
		 * @param backingConfig The configuration holding the actual config data.
		 * @param prefix The prefix prepended to all config keys.
		 */
		public DelegatingConfiguration(Configuration backingConfig, String prefix)
		{
			this.backingConfig = backingConfig;
			this.prefix = prefix;
		}

		// --------------------------------------------------------------------------------------------
		
		@Override
		public String getString(String key, String defaultValue) {
			return this.backingConfig.getString(this.prefix + key, defaultValue);
		}

		@Override
		public <T> Class<T> getClass(String key, Class<? extends T> defaultValue, Class<T> ancestor) {
			return this.backingConfig.getClass(this.prefix + key, defaultValue, ancestor);
		}

		@Override
		public Class<?> getClass(String key, Class<?> defaultValue) {
			return this.backingConfig.getClass(this.prefix + key, defaultValue);
		}

		@Override
		public void setClass(String key, Class<?> klazz) {
			this.backingConfig.setClass(this.prefix + key, klazz);
		}

		@Override
		public void setString(String key, String value) {
			this.backingConfig.setString(this.prefix + key, value);
		}

		@Override
		public int getInteger(String key, int defaultValue) {
			return this.backingConfig.getInteger(this.prefix + key, defaultValue);
		}

		@Override
		public void setInteger(String key, int value) {
			this.backingConfig.setInteger(this.prefix + key, value);
		}

		@Override
		public long getLong(String key, long defaultValue) {
			return this.backingConfig.getLong(this.prefix + key, defaultValue);
		}

		@Override
		public void setLong(String key, long value) {
			this.backingConfig.setLong(this.prefix + key, value);
		}

		@Override
		public boolean getBoolean(String key, boolean defaultValue) {
			return this.backingConfig.getBoolean(this.prefix + key, defaultValue);
		}

		@Override
		public void setBoolean(String key, boolean value) {
			this.backingConfig.setBoolean(this.prefix + key, value);
		}

		@Override
		public float getFloat(String key, float defaultValue) {
			return this.backingConfig.getFloat(this.prefix + key, defaultValue);
		}

		@Override
		public void setFloat(String key, float value) {
			this.backingConfig.setFloat(this.prefix + key, value);
		}

		@Override
		public Set<String> keySet()
		{
			final HashSet<String> set = new HashSet<String>();
			final int prefixLen = this.prefix == null ? 0 : this.prefix.length();
			
			for (String key : this.backingConfig.keySet()) {
				if (key.startsWith(this.prefix)) {
					set.add(key.substring(prefixLen));
				}
			}
			return set;
		}
		
		// --------------------------------------------------------------------------------------------

		@Override
		public void read(DataInput in) throws IOException
		{
			this.prefix = in.readUTF();
			this.backingConfig.read(in);
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(this.prefix);
			this.backingConfig.write(out);
		}
		
		// --------------------------------------------------------------------------------------------

		@Override
		public int hashCode()
		{
			return this.prefix.hashCode() ^ this.backingConfig.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj instanceof DelegatingConfiguration) {
				DelegatingConfiguration other = (DelegatingConfiguration) obj;
				return this.prefix.equals(other.prefix) && this.backingConfig.equals(other.backingConfig);
			}
			else return false;
		}
	}
}