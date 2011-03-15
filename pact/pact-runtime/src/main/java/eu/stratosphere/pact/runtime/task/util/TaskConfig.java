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
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * @author Erik Nijkamp
 * @author Fabian Hueske
 */
public class TaskConfig {

	public enum LocalStrategy {
		SORTMERGE,
		SORT,
		COMBININGSORT,
		HYBRIDHASH_FIRST,
		HYBRIDHASH_SECOND,
		MMHASH_FIRST,
		MMHASH_SECOND,
		NESTEDLOOP_BLOCKED_OUTER_FIRST,
		NESTEDLOOP_BLOCKED_OUTER_SECOND,
		NESTEDLOOP_STREAMED_OUTER_FIRST,
		NESTEDLOOP_STREAMED_OUTER_SECOND,
		NONE
	}

	public enum PostprocessingStrategy {

	}

	private static final String STUB_CLASS = "pact.stub.class";

	private static final String STUB_PARAM_PREFIX = "pact.stub.param.";

	private static final String INPUT_SHIP_STRATEGY = "pact.input.ship.strategy.";

	private static final String OUTPUT_SHIP_STRATEGY = "pact.output.ship.strategy.";

	private static final String LOCAL_STRATEGY = "pact.local.strategy";

	private static final String POSTPROCESSING_STRATEGY = "pact.postprocessing.strategy";

	private static final String NUM_INPUTS = "pact.inputs.number";

	private static final String NUM_OUTPUTS = "pact.outputs.number";

	private static final String SIZE_MEMORY = "pact.memory.size";

	private static final String NUM_FILEHANDLES = "pact.filehandles.num";

	protected final Configuration config;

	public TaskConfig(Configuration config) {
		this.config = config;
	}

	public void setStubClass(Class<? extends Stub<?, ?>> stubClass) {
		config.setString(STUB_CLASS, stubClass.getName());
	}

	public <T extends Stub<?, ?>> Class<? extends T> getStubClass(Class<T> stubClass, ClassLoader cl)
			throws ClassNotFoundException {
		String stubClassName = config.getString(STUB_CLASS, null);
		if (stubClassName == null) {
			throw new IllegalStateException("stub class missing");
		}
		return Class.forName(stubClassName, true, cl).asSubclass(stubClass);
	}

	public void setStubParameters(Configuration parameters) {
		for (String key : parameters.keySet()) {
			config.setString(STUB_PARAM_PREFIX + key, parameters.getString(key, null));
		}
	}

	public Configuration getStubParameters() {
		Configuration parameters = new Configuration();
		for (String key : config.keySet()) {
			if (key.startsWith(STUB_PARAM_PREFIX)) {
				parameters.setString(key.substring(STUB_PARAM_PREFIX.length()), config.getString(key, null));
			}
		}
		return parameters;
	}

	public void addInputShipStrategy(ShipStrategy strategy) {
		int inputCnt = config.getInteger(NUM_INPUTS, 0);
		config.setString(INPUT_SHIP_STRATEGY + (inputCnt++), strategy.name());
		config.setInteger(NUM_INPUTS, inputCnt);
	}

	public ShipStrategy getInputShipStrategy(int inputId) {
		int inputCnt = config.getInteger(NUM_INPUTS, -1);
		if (!(inputId < inputCnt)) {
			return null;
		}
		return ShipStrategy.valueOf(config.getString(INPUT_SHIP_STRATEGY + inputId, ""));
	}

	public void addOutputShipStrategy(ShipStrategy strategy) {
		int outputCnt = config.getInteger(NUM_OUTPUTS, 0);
		config.setString(OUTPUT_SHIP_STRATEGY + (outputCnt++), strategy.name());
		config.setInteger(NUM_OUTPUTS, outputCnt);
	}

	public ShipStrategy getOutputShipStrategy(int outputId) {
		int outputCnt = config.getInteger(NUM_OUTPUTS, -1);
		if (!(outputId < outputCnt)) {
			return null;
		}
		return ShipStrategy.valueOf(config.getString(OUTPUT_SHIP_STRATEGY + outputId, ""));
	}

	public void setLocalStrategy(LocalStrategy strategy) {
		config.setString(LOCAL_STRATEGY, strategy.name());
	}

	public LocalStrategy getLocalStrategy() {
		return LocalStrategy.valueOf(config.getString(LOCAL_STRATEGY, ""));
	}

	public void setPostprocessingStrategy(PostprocessingStrategy strategy) {
		config.setString(POSTPROCESSING_STRATEGY, strategy.name());
	}

	public PostprocessingStrategy getPostprocessingStrategy() {
		return PostprocessingStrategy.valueOf(config.getString(POSTPROCESSING_STRATEGY, ""));
	}

	public int getNumOutputs() {
		return config.getInteger(NUM_OUTPUTS, -1);
	}

	public int getNumInputs() {
		return config.getInteger(NUM_INPUTS, -1);
	}

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
		config.setInteger(NUM_FILEHANDLES, numFileHandles);
	}

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
}
