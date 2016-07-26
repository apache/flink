/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

/**
 * Configuration class which stores all relevant parameters required to set up the Pact tasks.
 */
public class TaskConfig implements Serializable {

	private static final long serialVersionUID = -2498884325640066272L;
	
	
	private static final String TASK_NAME = "taskname";
	
	// ------------------------------------ User Code ---------------------------------------------
	
	private static final String STUB_OBJECT = "udf";
	
	private static final String STUB_PARAM_PREFIX = "udf.param.";
	
	// -------------------------------------- Driver ----------------------------------------------
	
	private static final String DRIVER_CLASS = "driver.class";
	
	private static final String DRIVER_STRATEGY = "driver.strategy";
	
	private static final String DRIVER_COMPARATOR_FACTORY_PREFIX = "driver.comp.";
	
	private static final String DRIVER_COMPARATOR_PARAMETERS_PREFIX = "driver.comp.params.";
	
	private static final String DRIVER_PAIR_COMPARATOR_FACTORY = "driver.paircomp";
	
	private static final String DRIVER_MUTABLE_OBJECT_MODE = "diver.mutableobjects";

	// -------------------------------------- Inputs ----------------------------------------------

	private static final String NUM_INPUTS = "in.num";
	
	private static final String NUM_BROADCAST_INPUTS = "in.bc.num";
	
	/*
	 * If one input has multiple predecessors (bag union), multiple
	 * inputs must be grouped together. For a map or reduce there is
	 * one group and "pact.size.inputGroup.0" will be equal to
	 * "pact.in.num"
	 * 
	 * In the case of a dual input pact (eg. match) there might be
	 * 2 predecessors for the first group and one for the second group.
	 * Hence, "pact.in.num" would be 3, "pact.size.inputGroup.0"
	 * would be 2, and "pact.size.inputGroup.1" would be 1.
	 */
	private static final String INPUT_GROUP_SIZE_PREFIX = "in.groupsize.";
	
	private static final String BROADCAST_INPUT_GROUP_SIZE_PREFIX = "in.bc.groupsize.";
	
	private static final String INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "in.serializer.";
	
	private static final String BROADCAST_INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "in.bc.serializer.";
	
	private static final String INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "in.serializer.param.";
	
	private static final String BROADCAST_INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "in.bc.serializer.param.";
	
	private static final String INPUT_LOCAL_STRATEGY_PREFIX = "in.strategy.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX = "in.comparator.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX = "in.comparator.param.";
	
	private static final String INPUT_DAM_PREFIX = "in.dam.";
	
	private static final String INPUT_REPLAYABLE_PREFIX = "in.dam.replay.";
	
	private static final String INPUT_DAM_MEMORY_PREFIX = "in.dam.mem.";
	
	private static final String BROADCAST_INPUT_NAME_PREFIX = "in.broadcast.name.";
	
	
	// -------------------------------------- Outputs ---------------------------------------------
	
	private static final String OUTPUTS_NUM = "out.num";
	
	private static final String OUTPUT_TYPE_SERIALIZER_FACTORY = "out.serializer";
	
	private static final String OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "out.serializer.param.";
	
	private static final String OUTPUT_SHIP_STRATEGY_PREFIX = "out.shipstrategy.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX = "out.comp.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX = "out.comp.param.";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_CLASS = "out.distribution.class";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_PREFIX = "out.distribution.";
	
	private static final String OUTPUT_PARTITIONER = "out.partitioner.";
	
	// ------------------------------------- Chaining ---------------------------------------------
	
	private static final String CHAINING_NUM_STUBS = "chaining.num";
	
	private static final String CHAINING_TASKCONFIG_PREFIX = "chaining.taskconfig.";
	
	private static final String CHAINING_TASK_PREFIX = "chaining.task.";
	
	private static final String CHAINING_TASKNAME_PREFIX = "chaining.taskname.";
	
	// ------------------------------------ Memory & Co -------------------------------------------
	
	private static final String MEMORY_DRIVER = "memory.driver";
	
	private static final String MEMORY_INPUT_PREFIX = "memory.input.";
	
	private static final String FILEHANDLES_DRIVER = "filehandles.driver";
	
	private static final String FILEHANDLES_INPUT_PREFIX = "filehandles.input.";
	
	private static final String SORT_SPILLING_THRESHOLD_DRIVER = "sort-spill-threshold.driver";
	
	private static final String SORT_SPILLING_THRESHOLD_INPUT_PREFIX = "sort-spill-threshold.input.";

	private static final String USE_LARGE_RECORD_HANDLER = "sort-spill.large-record-handler";

	private static final boolean USE_LARGE_RECORD_HANDLER_DEFAULT = false;

	// ----------------------------------- Iterations ---------------------------------------------
	
	private static final String NUMBER_OF_ITERATIONS = "iterative.num-iterations";
	
	private static final String NUMBER_OF_EOS_EVENTS_PREFIX = "iterative.num-eos-events.";
	
	private static final String NUMBER_OF_EOS_EVENTS_BROADCAST_PREFIX = "iterative.num-eos-events.bc.";
	
	private static final String ITERATION_HEAD_ID = "iterative.head.id";
	
	private static final String ITERATION_WORKSET_MARKER = "iterative.is-workset";
	
	private static final String ITERATION_HEAD_INDEX_OF_PARTIAL_SOLUTION = "iterative.head.ps-input-index";
	
	private static final String ITERATION_HEAD_INDEX_OF_SOLUTIONSET = "iterative.head.ss-input-index";
	
	private static final String ITERATION_HEAD_BACKCHANNEL_MEMORY = "iterative.head.backchannel-memory";
	
	private static final String ITERATION_HEAD_SOLUTION_SET_MEMORY = "iterative.head.solutionset-memory";
	
	private static final String ITERATION_HEAD_FINAL_OUT_CONFIG_PREFIX = "iterative.head.out.";
	
	private static final String ITERATION_HEAD_SYNC_OUT_INDEX = "iterative.head.sync-index.";
	
	private static final String ITERATION_CONVERGENCE_CRITERION = "iterative.terminationCriterion";
	
	private static final String ITERATION_CONVERGENCE_CRITERION_AGG_NAME = "iterative.terminationCriterion.agg.name";
	
	private static final String ITERATION_NUM_AGGREGATORS = "iterative.num-aggs";
	
	private static final String ITERATION_AGGREGATOR_NAME_PREFIX = "iterative.agg.name.";
	
	private static final String ITERATION_AGGREGATOR_PREFIX = "iterative.agg.data.";
	
	private static final String ITERATION_SOLUTION_SET_SERIALIZER = "iterative.ss-serializer";
	
	private static final String ITERATION_SOLUTION_SET_SERIALIZER_PARAMETERS = "iterative.ss-serializer.params";
	
	private static final String ITERATION_SOLUTION_SET_COMPARATOR = "iterative.ss-comparator";
	
	private static final String ITERATION_SOLUTION_SET_COMPARATOR_PARAMETERS = "iterative.ss-comparator.params";
	
	private static final String ITERATION_SOLUTION_SET_UPDATE = "iterative.ss-update";
	
	private static final String ITERATION_SOLUTION_SET_UPDATE_SKIP_REPROBE = "iterative.ss-update-fast";

	private static final String ITERATION_SOLUTION_SET_UPDATE_WAIT = "iterative.ss-wait";

	private static final String ITERATION_WORKSET_UPDATE = "iterative.ws-update";
	
	private static final String SOLUTION_SET_OBJECTS = "itertive.ss.obj";

	// ---------------------------------- Miscellaneous -------------------------------------------
	
	private static final char SEPARATOR = '.';

	// --------------------------------------------------------------------------------------------
	//                         Members, Constructors, and Accessors
	// --------------------------------------------------------------------------------------------
	
	protected final Configuration config;			// the actual configuration holding the values
	
	/**
	 * Creates a new Task Config that wraps the given configuration.
	 * 
	 * @param config The configuration holding the actual values.
	 */
	public TaskConfig(Configuration config) {
		this.config = config;
	}
	
	/**
	 * Gets the configuration that holds the actual values encoded.
	 * 
	 * @return The configuration that holds the actual values
	 */
	public Configuration getConfiguration() {
		return this.config;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                       User Code
	// --------------------------------------------------------------------------------------------
	
	public void setTaskName(String name) {
		if (name != null) {
			this.config.setString(TASK_NAME, name);
		}
	}
	
	public String getTaskName() {
		return this.config.getString(TASK_NAME, null);
	}

	public boolean hasStubWrapper() {
		return this.config.containsKey(STUB_OBJECT);
	}
	
	
	public void setStubWrapper(UserCodeWrapper<?> wrapper) {
		try {
			InstantiationUtil.writeObjectToConfig(wrapper, this.config, STUB_OBJECT);
		} catch (IOException e) {
			throw new CorruptConfigurationException("Could not write the user code wrapper " + wrapper.getClass() + " : " + e.toString(), e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> UserCodeWrapper<T> getStubWrapper(ClassLoader cl) {
		try {
			return (UserCodeWrapper<T>) InstantiationUtil.readObjectFromConfig(this.config, STUB_OBJECT, cl);
		} catch (ClassNotFoundException e) {
			throw new CorruptConfigurationException("Could not read the user code wrapper: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new CorruptConfigurationException("Could not read the user code wrapper: " + e.getMessage(), e);
		}
	}
	
	public void setStubParameters(Configuration parameters) {
		this.config.addAll(parameters, STUB_PARAM_PREFIX);
	}

	public Configuration getStubParameters() {
		return new DelegatingConfiguration(this.config, STUB_PARAM_PREFIX);
	}
	
	public void setStubParameter(String key, String value) {
		this.config.setString(STUB_PARAM_PREFIX + key, value);
	}

	public String getStubParameter(String key, String defaultValue) {
		return this.config.getString(STUB_PARAM_PREFIX + key, defaultValue);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                      Driver
	// --------------------------------------------------------------------------------------------
	
	public void setDriver(@SuppressWarnings("rawtypes") Class<? extends Driver> driver) {
		this.config.setString(DRIVER_CLASS, driver.getName());
	}
	
	public <S extends Function, OT> Class<? extends Driver<S, OT>> getDriver() {
		final String className = this.config.getString(DRIVER_CLASS, null);
		if (className == null) {
			throw new CorruptConfigurationException("The pact driver class is missing.");
		}
		
		try {
			@SuppressWarnings("unchecked")
			final Class<Driver<S, OT>> pdClazz = (Class<Driver<S, OT>>) (Class<?>) Driver.class;
			return Class.forName(className).asSubclass(pdClazz);
		} catch (ClassNotFoundException cnfex) {
			throw new CorruptConfigurationException("The given driver class cannot be found.");
		} catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The given driver class does not implement the pact driver interface.");
		}
	}
	
	public void setDriverStrategy(DriverStrategy strategy) {
		this.config.setInteger(DRIVER_STRATEGY, strategy.ordinal());
	}
	
	public DriverStrategy getDriverStrategy() {
		final int ls = this.config.getInteger(DRIVER_STRATEGY, -1);
		if (ls == -1) {
			return DriverStrategy.NONE;
		} else if (ls < 0 || ls >= DriverStrategy.values().length) {
			throw new CorruptConfigurationException("Illegal driver strategy in configuration: " + ls);
		} else {
			return DriverStrategy.values()[ls];
		}
	}
	
	public void setMutableObjectMode(boolean mode) {
		this.config.setBoolean(DRIVER_MUTABLE_OBJECT_MODE, mode);
	}
	
	public boolean getMutableObjectMode() {
		return this.config.getBoolean(DRIVER_MUTABLE_OBJECT_MODE, false);
	}
	
	public void setDriverComparator(TypeComparatorFactory<?> factory, int inputNum) {
		setTypeComparatorFactory(factory, DRIVER_COMPARATOR_FACTORY_PREFIX + inputNum,
			DRIVER_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPARATOR);
	}
	
	public <T> TypeComparatorFactory<T> getDriverComparator(int inputNum, ClassLoader cl) {
		return getTypeComparatorFactory(DRIVER_COMPARATOR_FACTORY_PREFIX + inputNum,
			DRIVER_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPARATOR, cl);
	}
	
	public void setDriverPairComparator(TypePairComparatorFactory<?, ?> factory) {
		final Class<?> clazz = factory.getClass();
		InstantiationUtil.checkForInstantiation(clazz);
		this.config.setString(DRIVER_PAIR_COMPARATOR_FACTORY, clazz.getName());
	}
			
	public <T1, T2> TypePairComparatorFactory<T1, T2> getPairComparatorFactory(ClassLoader cl) {
		final String className = this.config.getString(DRIVER_PAIR_COMPARATOR_FACTORY, null);
		if (className == null) {
			return null;
		}
		
		@SuppressWarnings("unchecked")
		final Class<TypePairComparatorFactory<T1, T2>> superClass = (Class<TypePairComparatorFactory<T1, T2>>) (Class<?>) TypePairComparatorFactory.class;
		try {
			final Class<? extends TypePairComparatorFactory<T1, T2>> clazz = Class.forName(className, true, cl).asSubclass(superClass);
			return InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class '" + className + "', noted in the configuration as " +
				"pair comparator factory, could not be found. It is not part of the user code's class loader resources.");
		}
		catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the pair comparator factory " +
				"is no subclass of TypePairComparatorFactory.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Inputs
	// --------------------------------------------------------------------------------------------
	
	public void setInputLocalStrategy(int inputNum, LocalStrategy strategy) {
		this.config.setInteger(INPUT_LOCAL_STRATEGY_PREFIX + inputNum, strategy.ordinal());
	}
	
	public LocalStrategy getInputLocalStrategy(int inputNum) {
		final int ls = this.config.getInteger(INPUT_LOCAL_STRATEGY_PREFIX + inputNum, -1);
		if (ls == -1) {
			return LocalStrategy.NONE;
		} else if (ls < 0 || ls >= LocalStrategy.values().length) {
			throw new CorruptConfigurationException("Illegal local strategy in configuration: " + ls);
		} else {
			return LocalStrategy.values()[ls];
		}
	}
	
	public void setInputSerializer(TypeSerializerFactory<?> factory, int inputNum) {
		setTypeSerializerFactory(factory, INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum,
			INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPARATOR);
	}
	
	public void setBroadcastInputSerializer(TypeSerializerFactory<?> factory, int inputNum) {
		setTypeSerializerFactory(factory, BROADCAST_INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum,
			BROADCAST_INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPARATOR);
	}
	
	public <T> TypeSerializerFactory<T> getInputSerializer(int inputNum, ClassLoader cl) {
		return getTypeSerializerFactory(INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum,
			INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPARATOR, cl);
	}
	
	public <T> TypeSerializerFactory<T> getBroadcastInputSerializer(int inputNum, ClassLoader cl) {
		return getTypeSerializerFactory(BROADCAST_INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum,
			BROADCAST_INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPARATOR, cl);
	}
	
	public void setInputComparator(TypeComparatorFactory<?> factory, int inputNum) {
		setTypeComparatorFactory(factory, INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX + inputNum,
			INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPARATOR);
	}
	
	public <T> TypeComparatorFactory<T> getInputComparator(int inputNum, ClassLoader cl) {
		return getTypeComparatorFactory(INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX + inputNum,
			INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPARATOR, cl);
	}
	
	public int getNumInputs() {
		return this.config.getInteger(NUM_INPUTS, 0);
	}
	
	public int getNumBroadcastInputs() {
		return this.config.getInteger(NUM_BROADCAST_INPUTS, 0);
	}
	
	public int getGroupSize(int groupIndex) {
		return this.config.getInteger(INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	
	public int getBroadcastGroupSize(int groupIndex) {
		return this.config.getInteger(BROADCAST_INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	
	public void addInputToGroup(int groupIndex) {
		final String grp = INPUT_GROUP_SIZE_PREFIX + groupIndex;
		this.config.setInteger(grp, this.config.getInteger(grp, 0) + 1);
		this.config.setInteger(NUM_INPUTS, this.config.getInteger(NUM_INPUTS, 0) + 1);
	}
	
	public void addBroadcastInputToGroup(int groupIndex) {
		final String grp = BROADCAST_INPUT_GROUP_SIZE_PREFIX + groupIndex;
		if (!this.config.containsKey(grp)) {
			this.config.setInteger(NUM_BROADCAST_INPUTS, this.config.getInteger(NUM_BROADCAST_INPUTS, 0) + 1);
		}
		this.config.setInteger(grp, this.config.getInteger(grp, 0) + 1);
	}
	
	public void setInputAsynchronouslyMaterialized(int inputNum, boolean temp) {
		this.config.setBoolean(INPUT_DAM_PREFIX + inputNum, temp);
	}
	
	public boolean isInputAsynchronouslyMaterialized(int inputNum) {
		return this.config.getBoolean(INPUT_DAM_PREFIX + inputNum, false);
	}
	
	public void setInputCached(int inputNum, boolean persistent) {
		this.config.setBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, persistent);
	}
	
	public boolean isInputCached(int inputNum) {
		return this.config.getBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, false);
	}
	
	public void setRelativeInputMaterializationMemory(int inputNum, double relativeMemory) {
		this.config.setDouble(INPUT_DAM_MEMORY_PREFIX + inputNum, relativeMemory);
	}
	
	public double getRelativeInputMaterializationMemory(int inputNum) {
		return this.config.getDouble(INPUT_DAM_MEMORY_PREFIX + inputNum, 0);
	}
	
	public void setBroadcastInputName(String name, int groupIndex) {
		this.config.setString(BROADCAST_INPUT_NAME_PREFIX + groupIndex, name);
	}
	
	public String getBroadcastInputName(int groupIndex) {
		return this.config.getString(BROADCAST_INPUT_NAME_PREFIX + groupIndex, String.format("broadcastVar%04d", groupIndex));
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Outputs
	// --------------------------------------------------------------------------------------------
	
	public void addOutputShipStrategy(ShipStrategyType strategy) {
		final int outputCnt = this.config.getInteger(OUTPUTS_NUM, 0);
		this.config.setInteger(OUTPUT_SHIP_STRATEGY_PREFIX + outputCnt, strategy.ordinal());
		this.config.setInteger(OUTPUTS_NUM, outputCnt + 1);
	}
	
	public int getNumOutputs() {
		return this.config.getInteger(OUTPUTS_NUM, 0);
	}

	public ShipStrategyType getOutputShipStrategy(int outputNum) {
		// check how many outputs are encoded in the config
		final int outputCnt = this.config.getInteger(OUTPUTS_NUM, -1);
		if (outputCnt < 1) {
			throw new CorruptConfigurationException("No output ship strategies are specified in the configuration.");
		}
		
		// sanity range checks
		if (outputNum < 0 || outputNum >= outputCnt) {
			throw new IllegalArgumentException("Invalid index for output shipping strategy.");
		}
		
		final int strategy = this.config.getInteger(OUTPUT_SHIP_STRATEGY_PREFIX + outputNum, -1);
		if (strategy == -1) {
			throw new CorruptConfigurationException("No output shipping strategy in configuration for output " + outputNum);
		} else if (strategy < 0 || strategy >= ShipStrategyType.values().length) {
			throw new CorruptConfigurationException("Illegal output shipping strategy in configuration for output "
																			+ outputNum + ": " + strategy);
		} else {
			return ShipStrategyType.values()[strategy];
		}
	}
	
	public void setOutputSerializer(TypeSerializerFactory<?> factory) {
		setTypeSerializerFactory(factory, OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX);
	}
	
	public <T> TypeSerializerFactory<T> getOutputSerializer(ClassLoader cl) {
		return getTypeSerializerFactory(OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX, cl);
	}
	
	public void setOutputComparator(TypeComparatorFactory<?> factory, int outputNum) {
		setTypeComparatorFactory(factory, OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPARATOR);
	}
	
	public <T> TypeComparatorFactory<T> getOutputComparator(int outputNum, ClassLoader cl) {
		return getTypeComparatorFactory(OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPARATOR, cl);
	}
	
	public void setOutputDataDistribution(DataDistribution distribution, int outputNum) {
		this.config.setString(OUTPUT_DATA_DISTRIBUTION_CLASS, distribution.getClass().getName());
		
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
			
			distribution.write(out);
			config.setBytes(OUTPUT_DATA_DISTRIBUTION_PREFIX + outputNum, baos.toByteArray());
			
		}
		catch (IOException e) {
			throw new RuntimeException("Error serializing the DataDistribution: " + e.getMessage(), e);
		}
	}
	
	public DataDistribution getOutputDataDistribution(int outputNum, final ClassLoader cl) throws ClassNotFoundException {
		final String className = this.config.getString(OUTPUT_DATA_DISTRIBUTION_CLASS, null);
		if (className == null) {
			return null;
		}
		
		final Class<? extends DataDistribution> clazz;
		try {
			clazz = Class.forName(className, true, cl).asSubclass(DataDistribution.class);
		} catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the data distribution " +
					"is no subclass of DataDistribution.");
		}
		
		final DataDistribution distribution = InstantiationUtil.instantiate(clazz, DataDistribution.class);
		
		final byte[] stateEncoded = this.config.getBytes(OUTPUT_DATA_DISTRIBUTION_PREFIX + outputNum, null);
		if (stateEncoded == null) {
			throw new CorruptConfigurationException(
						"The configuration contained the data distribution type, but no serialized state.");
		}
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(stateEncoded);
		final DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
		
		try {
			distribution.read(in);
			return distribution;
		} catch (Exception ex) {
			throw new RuntimeException("The deserialization of the encoded data distribution state caused an error"
				+ (ex.getMessage() == null ? "." : ": " + ex.getMessage()), ex);
		}
	}
	
	public void setOutputPartitioner(Partitioner<?> partitioner, int outputNum) {
		try {
			InstantiationUtil.writeObjectToConfig(partitioner, config, OUTPUT_PARTITIONER + outputNum);
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not serialize custom partitioner.", t);
		}
	}
	
	public Partitioner<?> getOutputPartitioner(int outputNum, final ClassLoader cl) throws ClassNotFoundException {
		try {
			return (Partitioner<?>) InstantiationUtil.readObjectFromConfig(config, OUTPUT_PARTITIONER + outputNum, cl);
		}
		catch (ClassNotFoundException e) {
			throw e;
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not deserialize custom partitioner.", t);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------

	public void setRelativeMemoryDriver(double relativeMemorySize) {
		this.config.setDouble(MEMORY_DRIVER, relativeMemorySize);
	}

	public double getRelativeMemoryDriver() {
		return this.config.getDouble(MEMORY_DRIVER, 0);
	}
	
	public void setRelativeMemoryInput(int inputNum, double relativeMemorySize) {
		this.config.setDouble(MEMORY_INPUT_PREFIX + inputNum, relativeMemorySize);
	}

	public double getRelativeMemoryInput(int inputNum) {
		return this.config.getDouble(MEMORY_INPUT_PREFIX + inputNum, 0);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setFilehandlesDriver(int filehandles) {
		if (filehandles < 2) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(FILEHANDLES_DRIVER, filehandles);
	}

	public int getFilehandlesDriver() {
		return this.config.getInteger(FILEHANDLES_DRIVER, -1);
	}
	
	public void setFilehandlesInput(int inputNum, int filehandles) {
		if (filehandles < 2) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(FILEHANDLES_INPUT_PREFIX + inputNum, filehandles);
	}

	public int getFilehandlesInput(int inputNum) {
		return this.config.getInteger(FILEHANDLES_INPUT_PREFIX + inputNum, -1);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setSpillingThresholdDriver(float threshold) {
		if (threshold < 0.0f || threshold > 1.0f) {
			throw new IllegalArgumentException();
		}
		this.config.setFloat(SORT_SPILLING_THRESHOLD_DRIVER, threshold);
	}

	public float getSpillingThresholdDriver() {
		return this.config.getFloat(SORT_SPILLING_THRESHOLD_DRIVER, 0.7f);
	}
	
	public void setSpillingThresholdInput(int inputNum, float threshold) {
		if (threshold < 0.0f || threshold > 1.0f) {
			throw new IllegalArgumentException();
		}
		this.config.setFloat(SORT_SPILLING_THRESHOLD_INPUT_PREFIX + inputNum, threshold);
	}

	public float getSpillingThresholdInput(int inputNum) {
		return this.config.getFloat(SORT_SPILLING_THRESHOLD_INPUT_PREFIX + inputNum, 0.7f);
	}

	public void setUseLargeRecordHandler(boolean useLargeRecordHandler) {
		this.config.setBoolean(USE_LARGE_RECORD_HANDLER, useLargeRecordHandler);
	}

	public boolean getUseLargeRecordHandler() {
		return this.config.getBoolean(USE_LARGE_RECORD_HANDLER, USE_LARGE_RECORD_HANDLER_DEFAULT);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Parameters for Function Chaining
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfChainedStubs() {
		return this.config.getInteger(CHAINING_NUM_STUBS, 0);
	}
	
	public void addChainedTask(@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> chainedTaskClass, TaskConfig conf, String taskName)
	{
		int numChainedYet = this.config.getInteger(CHAINING_NUM_STUBS, 0);
		
		this.config.setString(CHAINING_TASK_PREFIX + numChainedYet, chainedTaskClass.getName());
		this.config.addAll(conf.config, CHAINING_TASKCONFIG_PREFIX + numChainedYet + SEPARATOR);
		this.config.setString(CHAINING_TASKNAME_PREFIX + numChainedYet, taskName);
		
		this.config.setInteger(CHAINING_NUM_STUBS, ++numChainedYet);
	}
	
	public TaskConfig getChainedStubConfig(int chainPos) {
		return new TaskConfig(new DelegatingConfiguration(this.config, CHAINING_TASKCONFIG_PREFIX + chainPos + SEPARATOR));
	}

	public Class<? extends ChainedDriver<?, ?>> getChainedTask(int chainPos) {
		final String className = this.config.getString(CHAINING_TASK_PREFIX + chainPos, null);
		if (className == null) {
			throw new IllegalStateException("Chained Task Class missing");
		}
		
		@SuppressWarnings("unchecked")
		final Class<ChainedDriver<?, ?>> clazz = (Class<ChainedDriver<?, ?>>) (Class<?>) ChainedDriver.class;
		try {
			return Class.forName(className).asSubclass(clazz);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	public String getChainedTaskName(int chainPos) {
		return this.config.getString(CHAINING_TASKNAME_PREFIX + chainPos, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                      Iterations
	// --------------------------------------------------------------------------------------------

	public void setNumberOfIterations(int numberOfIterations) {
		if (numberOfIterations <= 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(NUMBER_OF_ITERATIONS, numberOfIterations);
	}

	public int getNumberOfIterations() {
		int numberOfIterations = this.config.getInteger(NUMBER_OF_ITERATIONS, 0);
		if (numberOfIterations <= 0) {
			throw new IllegalArgumentException();
		}
		return numberOfIterations;
	}
	
	public void setIterationHeadPartialSolutionOrWorksetInputIndex(int inputIndex) {
		if (inputIndex < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(ITERATION_HEAD_INDEX_OF_PARTIAL_SOLUTION, inputIndex);
	}
	
	public int getIterationHeadPartialSolutionOrWorksetInputIndex() {
		int index = this.config.getInteger(ITERATION_HEAD_INDEX_OF_PARTIAL_SOLUTION, -1);
		if (index < 0) {
			throw new IllegalArgumentException();
		}
		return index;
	}
	
	public void setIterationHeadSolutionSetInputIndex(int inputIndex) {
		if (inputIndex < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(ITERATION_HEAD_INDEX_OF_SOLUTIONSET, inputIndex);
	}
	
	public int getIterationHeadSolutionSetInputIndex() {
		int index = this.config.getInteger(ITERATION_HEAD_INDEX_OF_SOLUTIONSET, -1);
		if (index < 0) {
			throw new IllegalArgumentException();
		}
		return index;
	}
	
	public void setRelativeBackChannelMemory(double relativeMemory) {
		if (relativeMemory < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setDouble(ITERATION_HEAD_BACKCHANNEL_MEMORY, relativeMemory);
	}

	public double getRelativeBackChannelMemory() {
		double relativeBackChannelMemory = this.config.getDouble(ITERATION_HEAD_BACKCHANNEL_MEMORY, 0);
		if (relativeBackChannelMemory <= 0) {
			throw new IllegalArgumentException();
		}
		return relativeBackChannelMemory;
	}
	
	public void setRelativeSolutionSetMemory(double relativeMemory) {
		if (relativeMemory < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setDouble(ITERATION_HEAD_SOLUTION_SET_MEMORY, relativeMemory);
	}

	public double getRelativeSolutionSetMemory() {
		double backChannelMemory = this.config.getDouble(ITERATION_HEAD_SOLUTION_SET_MEMORY, 0);
		if (backChannelMemory <= 0) {
			throw new IllegalArgumentException();
		}
		return backChannelMemory;
	}

	public boolean isIterativeInputGate(int inputGateIndex) {
		return getNumberOfEventsUntilInterruptInIterativeGate(inputGateIndex) > 0;
	}

	public void setGateIterativeWithNumberOfEventsUntilInterrupt(int inputGateIndex, int numEvents) {
		if (inputGateIndex < 0) {
			throw new IllegalArgumentException();
		}
		if (numEvents <= 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(NUMBER_OF_EOS_EVENTS_PREFIX + inputGateIndex, numEvents);
	}

	public int getNumberOfEventsUntilInterruptInIterativeGate(int inputGateIndex) {
		if (inputGateIndex < 0) {
			throw new IllegalArgumentException();
		}
		return this.config.getInteger(NUMBER_OF_EOS_EVENTS_PREFIX + inputGateIndex, 0);
	}
	
	public void setBroadcastGateIterativeWithNumberOfEventsUntilInterrupt(int bcGateIndex, int numEvents) {
		if (bcGateIndex < 0) {
			throw new IllegalArgumentException();
		}
		if (numEvents <= 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(NUMBER_OF_EOS_EVENTS_BROADCAST_PREFIX + bcGateIndex, numEvents);
	}

	public int getNumberOfEventsUntilInterruptInIterativeBroadcastGate(int bcGateIndex) {
		if (bcGateIndex < 0) {
			throw new IllegalArgumentException();
		}
		return this.config.getInteger(NUMBER_OF_EOS_EVENTS_BROADCAST_PREFIX + bcGateIndex, 0);
	}
	
	public void setIterationId(int id) {
		if (id < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(ITERATION_HEAD_ID, id);
	}
	
	public int getIterationId() {
		int id = this.config.getInteger(ITERATION_HEAD_ID, -1);
		if (id == -1) {
			throw new CorruptConfigurationException("Iteration head ID is missing.");
		}
		return id;
	}
	
	public void setIsWorksetIteration() {
		this.config.setBoolean(ITERATION_WORKSET_MARKER, true);
	}
	
	public boolean getIsWorksetIteration() {
		return this.config.getBoolean(ITERATION_WORKSET_MARKER, false);
	}
	
	public void setIterationHeadIndexOfSyncOutput(int outputIndex) {
		if (outputIndex < 0) {
			throw new IllegalArgumentException();
		}
		this.config.setInteger(ITERATION_HEAD_SYNC_OUT_INDEX, outputIndex);
	}
	
	public int getIterationHeadIndexOfSyncOutput() {
		int outputIndex = this.config.getInteger(ITERATION_HEAD_SYNC_OUT_INDEX, -1);
		if (outputIndex < 0) {
			throw new IllegalArgumentException();
		}
		return outputIndex;
	}
	
	public void setIterationHeadFinalOutputConfig(TaskConfig conf) {
		this.config.addAll(conf.config, ITERATION_HEAD_FINAL_OUT_CONFIG_PREFIX);
	}
	
	public TaskConfig getIterationHeadFinalOutputConfig() {
		return new TaskConfig(new DelegatingConfiguration(this.config, ITERATION_HEAD_FINAL_OUT_CONFIG_PREFIX));
	}
	
	public void setSolutionSetSerializer(TypeSerializerFactory<?> factory) {
		setTypeSerializerFactory(factory, ITERATION_SOLUTION_SET_SERIALIZER,
			ITERATION_SOLUTION_SET_SERIALIZER_PARAMETERS);
	}
	
	public <T> TypeSerializerFactory<T> getSolutionSetSerializer(ClassLoader cl) {
		return getTypeSerializerFactory(ITERATION_SOLUTION_SET_SERIALIZER,
			ITERATION_SOLUTION_SET_SERIALIZER_PARAMETERS, cl);
	}
	
	public void setSolutionSetComparator(TypeComparatorFactory<?> factory) {
		setTypeComparatorFactory(factory, ITERATION_SOLUTION_SET_COMPARATOR,
			ITERATION_SOLUTION_SET_COMPARATOR_PARAMETERS);
	}
	
	public <T> TypeComparatorFactory<T> getSolutionSetComparator(ClassLoader cl) {
		return getTypeComparatorFactory(ITERATION_SOLUTION_SET_COMPARATOR,
			ITERATION_SOLUTION_SET_COMPARATOR_PARAMETERS, cl);
	}

	public void addIterationAggregator(String name, Aggregator<?> aggregator) {
		int num = this.config.getInteger(ITERATION_NUM_AGGREGATORS, 0);
		this.config.setString(ITERATION_AGGREGATOR_NAME_PREFIX + num, name);
		try {
				InstantiationUtil.writeObjectToConfig(aggregator, this.config, ITERATION_AGGREGATOR_PREFIX + num);
		} catch (IOException e) {
				throw new RuntimeException("Error while writing the aggregator object to the task configuration.");
		}
		this.config.setInteger(ITERATION_NUM_AGGREGATORS, num + 1);
	}
	
	public void addIterationAggregators(Collection<AggregatorWithName<?>> aggregators) {
		int num = this.config.getInteger(ITERATION_NUM_AGGREGATORS, 0);
		for (AggregatorWithName<?> awn : aggregators) {
			this.config.setString(ITERATION_AGGREGATOR_NAME_PREFIX + num, awn.getName());
			try {
				InstantiationUtil.writeObjectToConfig(awn.getAggregator(), this.config, ITERATION_AGGREGATOR_PREFIX + num);
			} catch (IOException e) {
				throw new RuntimeException("Error while writing the aggregator object to the task configuration.");
			}
			num++;
		}
		this.config.setInteger(ITERATION_NUM_AGGREGATORS, num);
	}
	
	@SuppressWarnings("unchecked")
	public Collection<AggregatorWithName<?>> getIterationAggregators(ClassLoader cl) {
		final int numAggs = this.config.getInteger(ITERATION_NUM_AGGREGATORS, 0);
		if (numAggs == 0) {
			return Collections.emptyList();
		}
		
		List<AggregatorWithName<?>> list = new ArrayList<AggregatorWithName<?>>(numAggs);
		for (int i = 0; i < numAggs; i++) {
			Aggregator<Value> aggObj;
			try {
				aggObj = (Aggregator<Value>) InstantiationUtil.readObjectFromConfig(
						this.config, ITERATION_AGGREGATOR_PREFIX + i, cl);
			} catch (IOException e) {
					throw new RuntimeException("Error while reading the aggregator object from the task configuration.");
			} catch (ClassNotFoundException e) {
					throw new RuntimeException("Error while reading the aggregator object from the task configuration. " +
				"Aggregator class not found.");
			}
			if (aggObj == null) {
				throw new RuntimeException("Missing config entry for aggregator.");
			}
			String name = this.config.getString(ITERATION_AGGREGATOR_NAME_PREFIX + i, null);
			if (name == null) {
				throw new RuntimeException("Missing config entry for aggregator.");
			}
			list.add(new AggregatorWithName<Value>(name, aggObj));
		}
		return list;
	}
	
	public void setConvergenceCriterion(String aggregatorName, ConvergenceCriterion<?> convCriterion) {
		try {
			InstantiationUtil.writeObjectToConfig(convCriterion, this.config, ITERATION_CONVERGENCE_CRITERION);
		} catch (IOException e) {
			throw new RuntimeException("Error while writing the convergence criterion object to the task configuration.");
		}
		this.config.setString(ITERATION_CONVERGENCE_CRITERION_AGG_NAME, aggregatorName);
	}

	@SuppressWarnings("unchecked")
	public <T extends Value> ConvergenceCriterion<T> getConvergenceCriterion(ClassLoader cl) {
		ConvergenceCriterion<T> convCriterionObj = null;
		try {
			convCriterionObj = (ConvergenceCriterion<T>) InstantiationUtil.readObjectFromConfig(
			this.config, ITERATION_CONVERGENCE_CRITERION, cl);
		} catch (IOException e) {
			throw new RuntimeException("Error while reading the covergence criterion object from the task configuration.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error while reading the covergence criterion object from the task configuration. " +
					"ConvergenceCriterion class not found.");
		}
		if (convCriterionObj == null) {
			throw new NullPointerException();
		}
		return convCriterionObj;
	}

	public boolean usesConvergenceCriterion() {
		return config.getBytes(ITERATION_CONVERGENCE_CRITERION, null) != null;
	}
	
	public String getConvergenceCriterionAggregatorName() {
		return this.config.getString(ITERATION_CONVERGENCE_CRITERION_AGG_NAME, null);
	}
	
	public void setIsSolutionSetUpdate() {
		this.config.setBoolean(ITERATION_SOLUTION_SET_UPDATE, true);
	}
	
	public boolean getIsSolutionSetUpdate() {
		return this.config.getBoolean(ITERATION_SOLUTION_SET_UPDATE, false);
	}
	
	public void setIsSolutionSetUpdateWithoutReprobe() {
		this.config.setBoolean(ITERATION_SOLUTION_SET_UPDATE_SKIP_REPROBE, true);
	}
	
	public boolean getIsSolutionSetUpdateWithoutReprobe() {
		return this.config.getBoolean(ITERATION_SOLUTION_SET_UPDATE_SKIP_REPROBE, false);
	}

	public void setWaitForSolutionSetUpdate() {
		this.config.setBoolean(ITERATION_SOLUTION_SET_UPDATE_WAIT, true);
	}

	public boolean getWaitForSolutionSetUpdate() {
		return this.config.getBoolean(ITERATION_SOLUTION_SET_UPDATE_WAIT, false);
	}

	public void setIsWorksetUpdate() {
		this.config.setBoolean(ITERATION_WORKSET_UPDATE, true);
	}

	public boolean getIsWorksetUpdate() {
		return this.config.getBoolean(ITERATION_WORKSET_UPDATE, false);
	}

	// --------------------------------------------------------------------------------------------
	//                                    Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private void setTypeSerializerFactory(TypeSerializerFactory<?> factory,
			String classNameKey, String parametersPrefix)
	{
		// sanity check the factory type
		InstantiationUtil.checkForInstantiation(factory.getClass());
		
		// store the type
		this.config.setString(classNameKey, factory.getClass().getName());
		// store the parameters
		final DelegatingConfiguration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		factory.writeParametersToConfig(parameters);
	}
	
	private <T> TypeSerializerFactory<T> getTypeSerializerFactory(String classNameKey, String parametersPrefix, ClassLoader cl) {
		// check the class name
		final String className = this.config.getString(classNameKey, null);
		if (className == null) {
			return null;
		}
		
		// instantiate the class
		@SuppressWarnings("unchecked")
		final Class<TypeSerializerFactory<T>> superClass = (Class<TypeSerializerFactory<T>>) (Class<?>) TypeSerializerFactory.class;
		final TypeSerializerFactory<T> factory;
		try {
			Class<? extends TypeSerializerFactory<T>> clazz = Class.forName(className, true, cl).asSubclass(superClass);
			factory = InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class '" + className + "', noted in the configuration as " +
					"serializer factory, could not be found. It is not part of the user code's class loader resources.");
		}
		catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the serializer factory " +
					"is no subclass of TypeSerializerFactory.");
		}
		
		// parameterize the comparator factory
		final Configuration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		try {
			factory.readParametersFromConfig(parameters, cl);
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The type serializer factory could not load its parameters from the " +
					"configuration due to missing classes.", cnfex);
		}
		
		return factory;
	}
	
	private void setTypeComparatorFactory(TypeComparatorFactory<?> factory,
			String classNameKey, String parametersPrefix)
	{
		// sanity check the factory type
		InstantiationUtil.checkForInstantiation(factory.getClass());
		
		// store the type
		this.config.setString(classNameKey, factory.getClass().getName());
		// store the parameters
		final DelegatingConfiguration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		factory.writeParametersToConfig(parameters);
	}
	
	private <T> TypeComparatorFactory<T> getTypeComparatorFactory(String classNameKey, String parametersPrefix, ClassLoader cl) {
		// check the class name
		final String className = this.config.getString(classNameKey, null);
		if (className == null) {
			return null;
		}
		
		// instantiate the class
		@SuppressWarnings("unchecked")
		final Class<TypeComparatorFactory<T>> superClass = (Class<TypeComparatorFactory<T>>) (Class<?>) TypeComparatorFactory.class;
		final TypeComparatorFactory<T> factory;
		try {
			Class<? extends TypeComparatorFactory<T>> clazz = Class.forName(className, true, cl).asSubclass(superClass);
			factory = InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class '" + className + "', noted in the configuration as " +
					"comparator factory, could not be found. It is not part of the user code's class loader resources.");
		}
		catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the comparator factory " +
					"is no subclass of TypeComparatorFactory.");
		}
		
		// parameterize the comparator factory
		final Configuration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		try {
			factory.readParametersFromConfig(parameters, cl);
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The type serializer factory could not load its parameters from the " +
					"configuration due to missing classes.", cnfex);
		}
		
		return factory;
	}
	
	public void setSolutionSetUnmanaged(boolean unmanaged) {
		config.setBoolean(SOLUTION_SET_OBJECTS, unmanaged);
	}
	
	public boolean isSolutionSetUnmanaged() {
		return config.getBoolean(SOLUTION_SET_OBJECTS, false);
	}

}
