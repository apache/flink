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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
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
		// already grouped input, within a key values are crossed in a nested loop fasion
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
	
	private static final String INPUT_SHIP_NUM_KEYS_PREFIX = "pact.input.numkeys.";
	
	private static final String INPUT_SHIP_KEY_POS_PREFIX = "pact.input.keypos.";
	
	private static final String INPUT_SHIP_KEY_CLASS_PREFIX = "pact.input.keyclass.";

	private static final String OUTPUT_SHIP_STRATEGY = "pact.output.shipstrategy";
	
	private static final String OUTPUT_SHIP_NUM_KEYS_PREFIX = "pact.output.numkeys.";
	
	private static final String OUTPUT_SHIP_KEY_POS_PREFIX = "pact.output.keypos.";
	
	private static final String OUTPUT_SHIP_KEY_CLASS_PREFIX = "pact.output.keyclass.";

	private static final String LOCAL_STRATEGY = "pact.local.strategy";

	private static final String NUM_INPUTS = "pact.inputs.number";

	private static final String NUM_OUTPUTS = "pact.outputs.number";

	private static final String SIZE_MEMORY = "pact.memory.size";

	private static final String NUM_FILEHANDLES = "pact.filehandles.num";
	
	private static final String SORT_SPILLING_THRESHOLD = "pact.sort.spillthreshold";

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
		for (String key : parameters.keySet()) {
			this.config.setString(STUB_PARAM_PREFIX + key, parameters.getString(key, null));
		}
	}

	public Configuration getStubParameters()
	{
		Configuration parameters = new Configuration();
		for (String key : config.keySet()) {
			if (key.startsWith(STUB_PARAM_PREFIX)) {
				parameters.setString(key.substring(STUB_PARAM_PREFIX.length()), config.getString(key, null));
			}
		}
		return parameters;
	}
	
	public void setStubParameter(String key, String value)
	{
		config.setString(STUB_PARAM_PREFIX + key, value);
	}

	public String getStubParameter(String key, String defaultValue)
	{
		return config.getString(STUB_PARAM_PREFIX + key, defaultValue);
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
		config.setString(LOCAL_STRATEGY, strategy.name());
	}

	public LocalStrategy getLocalStrategy() {
		return LocalStrategy.valueOf(config.getString(LOCAL_STRATEGY, ""));
	}
	
	public void setLocalStrategyKeyParameters(int inputNum, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{			
		this.config.setInteger(INPUT_SHIP_NUM_KEYS_PREFIX + inputNum, keyPositions.length);
		for (int i = 0; i < keyPositions.length; i++) {
			this.config.setInteger(INPUT_SHIP_KEY_POS_PREFIX + inputNum + '.' + i, keyPositions[i]);
			this.config.setString(INPUT_SHIP_KEY_CLASS_PREFIX + inputNum + '.' + i, keyTypes[i].getName());
		}
	}
	
	public int[] getLocalStrategyKeyPositions(int inputNum)
	{		
		final int numKeys = this.config.getInteger(INPUT_SHIP_NUM_KEYS_PREFIX + inputNum, -1);
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
	
	public Class<? extends Key>[] getLocalStrategyKeyClasses(int inputNum, ClassLoader cl)
	throws ClassNotFoundException, ClassCastException
	{
		final int numKeys = this.config.getInteger(INPUT_SHIP_NUM_KEYS_PREFIX + inputNum, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = (Class<? extends Key>[]) new Class[numKeys];
		for (int i = 0; i < numKeys; i++) {
			String name = this.config.getString(INPUT_SHIP_KEY_CLASS_PREFIX + inputNum + '.' + i, null);
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
		this.config.setString(OUTPUT_SHIP_STRATEGY + outputCnt, strategy.name());
		outputCnt++;
		this.config.setInteger(NUM_OUTPUTS, outputCnt);
	}
	
	public void addOutputShipStrategy(ShipStrategy strategy, int[] keyPositions, Class<? extends Key>[] keyTypes)
	{
		int outputCnt = config.getInteger(NUM_OUTPUTS, 0);
		
		this.config.setString(OUTPUT_SHIP_STRATEGY + outputCnt, strategy.name());		
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
		return ShipStrategy.valueOf(this.config.getString(OUTPUT_SHIP_STRATEGY + outputId, ""));
	}
	
	public int[] getOutputShipKeyPositions(int outputId)
	{
		final int outputCnt = this.config.getInteger(NUM_OUTPUTS, -1);
		if (!(outputId < outputCnt)) {
			return null;
		}
		
		final int numKeys = this.config.getInteger(OUTPUT_SHIP_NUM_KEYS_PREFIX + outputCnt, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		final int[] keyPos = new int[numKeys];
		for (int i = 0; i < numKeys; i++) {
			int p = this.config.getInteger(OUTPUT_SHIP_KEY_POS_PREFIX + outputCnt + '.' + i, -1);
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
		
		final int numKeys = this.config.getInteger(OUTPUT_SHIP_NUM_KEYS_PREFIX + outputCnt, -1);
		if (numKeys <= 0) {
			return null;
		}
		
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = (Class<? extends Key>[]) new Class[numKeys];
		for (int i = 0; i < numKeys; i++) {
			String name = this.config.getString(OUTPUT_SHIP_KEY_CLASS_PREFIX + outputCnt + '.' + i, null);
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
}
