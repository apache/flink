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
		SORT_FIRST_MERGE,
		SORT_SECOND_MERGE,
		MERGE,
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

	private static final String NUM_SORT_BUFFER = "pact.sortbuffer.number";

	private static final String SIZE_SORT_BUFFER = "pact.sortbuffer.size";

	private static final String SIZE_IO_BUFFER = "pact.iobuffer.size";

	private static final String MERGE_FACTOR = "pact.merge.factor";

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
	 * Sets the number of sort buffers to use by task
	 * 
	 * @param numSortBuffers
	 *        number of sort buffers to use by task
	 */
	public void setNumSortBuffer(int numSortBuffers) {
		config.setInteger(NUM_SORT_BUFFER, numSortBuffers);
	}

	/**
	 * Sets the size of one sort buffer
	 * 
	 * @param sizeSortBuffers
	 *        size of one sort buffer in MB
	 */
	public void setSortBufferSize(int sizeSortBuffers) {
		config.setInteger(SIZE_SORT_BUFFER, sizeSortBuffers);
	}

	/**
	 * Sets the memory to be used for IO buffering
	 * 
	 * @param sizeIOBuffer
	 *        size of memory to be used in MB
	 */
	public void setIOBufferSize(int sizeIOBuffer) {
		config.setInteger(SIZE_IO_BUFFER, sizeIOBuffer);
	}

	/**
	 * Sets the maximum number of files to be merged in merge phase.
	 * 
	 * @param mergeFactor
	 *        maximum number of files to be merged
	 */
	public void setMergeFactor(int mergeFactor) {
		config.setInteger(MERGE_FACTOR, mergeFactor);
	}

	public int getNumSortBuffer() {
		return config.getInteger(NUM_SORT_BUFFER, -1);
	}

	public int getSortBufferSize() {
		return config.getInteger(SIZE_SORT_BUFFER, -1);
	}

	public int getIOBufferSize() {
		return config.getInteger(SIZE_IO_BUFFER, -1);
	}

	public int getMergeFactor() {
		return config.getInteger(MERGE_FACTOR, -1);
	}
}
