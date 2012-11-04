/***********************************************************************************************************************
 *
 * Copyright (C) 2010 - 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;

/**
 * Configuration class which stores all relevant parameters required to set up the Pact tasks.
 */
public class TaskConfig
{	
	// ------------------------------------ User Code ---------------------------------------------
	
	private static final String STUB_CLASS = "pact.stub.class";
	
	private static final String STUB_PARAM_PREFIX = "pact.stub.param.";
	
	// -------------------------------------- Driver ----------------------------------------------
	
	private static final String DRIVER_CLASS = "pact.driver.class";
	
	private static final String DRIVER_STRATEGY = "pact.driver.strategy";
	
	private static final String DRIVER_COMPARATOR_FACTORY_PREFIX = "pact.driver.comp.";
	
	private static final String DRIVER_COMPARATOR_PARAMETERS_PREFIX = "pact.driver.comp.params.";
	
	private static final String DRIVER_PAIR_COMPARATOR_FACTORY = "pact.driver.paircomp";

	// -------------------------------------- Inputs ----------------------------------------------

	private static final String NUM_INPUTS = "pact.in.num";
	
	/*
	 * If one input has multiple predecessors (bag union), multiple
	 * inputs must be grouped together. For a map or reduce there is
	 * one group and "pact.size.inputGroup.1" will be equal to
	 * "pact.inputs.number"
	 * 
	 * In the case of a dual input pact (eg. match) there might be
	 * 2 predecessors for the first group and one for the second group.
	 * Hence, "pact.inputs.number" would be 3, "pact.size.inputGroup.1"
	 * would be 2, and "pact.size.inputGroup.2" would be 1.
	 */
	private static final String INPUT_GROUP_SIZE_PREFIX = "pact.in.groupsize.";
	
	private static final String INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "pact.in.serializer.";
	
	private static final String INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "pact.in.serializer.param.";
	
	private static final String INPUT_LOCAL_STRATEGY_PREFIX = "pact.in.strategy.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX = "pact.in.comparator.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX = "pact.in.comparator.param.";
	
	private static final String INPUT_DAM_PREFIX = "pact.in.dam.";
	
	private static final String INPUT_REPLAYABLE_PREFIX = "pact.in.replay.";
	
	private static final String INPUT_DAM_REPLAY_MEMORY_PREFIX = "pact.in.temp.mem.";
	
	
	// -------------------------------------- Outputs ---------------------------------------------
	
	private static final String OUTPUTS_NUM = "pact.out.num";
	
	private static final String OUTPUT_TYPE_SERIALIZER_FACTORY = "pact.out.serializer";
	
	private static final String OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "pact.out.serializer.param.";
	
	private static final String OUTPUT_SHIP_STRATEGY_PREFIX = "pact.out.shipstrategy.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX = "pact.out.comp.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX = "pact.out.comp.param.";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_CLASS = "pact.out.distribution.class";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_STATE = "pact.out.distribution.state";
	
	// ------------------------------------- Chaining ---------------------------------------------
	
	private static final String CHAINING_NUM_STUBS = "pact.chaining.num";
	
	private static final String CHAINING_TASKCONFIG_PREFIX = "pact.chaining.taskconfig.";
	
	private static final String CHAINING_TASK_PREFIX = "pact.chaining.task.";
	
	private static final String CHAINING_TASKNAME_PREFIX = "pact.chaining.taskname.";
	
	// ------------------------------------ Memory & Co -------------------------------------------
	
	private static final String MEMORY_DRIVER = "pact.memory.driver";
	
	private static final String MEMORY_INPUT_PREFIX = "pact.memory.input.";
	
	private static final String FILEHANDLES_DRIVER = "pact.filehandles.driver";
	
	private static final String FILEHANDLES_INPUT_PREFIX = "pact.filehandles.input.";
	
	private static final String SORT_SPILLING_THRESHOLD_DRIVER = "pact.sort-spill-threshold.driver";
	
	private static final String SORT_SPILLING_THRESHOLD_INPUT_PREFIX = "pact.sort-spill-threshold.input.";
	
	// ---------------------------------- Miscellaneous -------------------------------------------
	
	private static final char SEPERATOR = '.';

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

	public void setStubClass(Class<?> stubClass) {
		this.config.setString(STUB_CLASS, stubClass.getName());
	}

	public <T> Class<? extends T> getStubClass(Class<T> superClass, ClassLoader cl)
		throws ClassNotFoundException, ClassCastException
	{
		final String stubClassName = this.config.getString(STUB_CLASS, null);
		if (stubClassName == null) {
			throw new CorruptConfigurationException("The stub class is missing.");
		}
		return Class.forName(stubClassName, true, cl).asSubclass(superClass);
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
	
	public void setDriver(@SuppressWarnings("rawtypes") Class<? extends PactDriver> driver) {
		this.config.setString(DRIVER_CLASS, driver.getName());
	}
	
	public <S extends Stub, OT> Class<? extends PactDriver<S, OT>> getDriver() {
		final String className = this.config.getString(DRIVER_CLASS, null);
		if (className == null) {
			throw new CorruptConfigurationException("The pact driver class is missing.");
		}
		
		try {
			@SuppressWarnings("unchecked")
			final Class<PactDriver<S, OT>> pdClazz = (Class<PactDriver<S, OT>>) (Class<?>) PactDriver.class;
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
	
	public void setDriverComparator(TypeComparatorFactory<?> factory, int inputNum) {
		setTypeComparatorFactory(factory, DRIVER_COMPARATOR_FACTORY_PREFIX + inputNum,
			DRIVER_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPERATOR);
	}
	
	public <T> TypeComparatorFactory<T> getDriverComparator(int inputNum, ClassLoader cl) {
		return getTypeComparatorFactory(DRIVER_COMPARATOR_FACTORY_PREFIX + inputNum,
			DRIVER_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPERATOR, cl);
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
			INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPERATOR);
	}
	
	public <T> TypeSerializerFactory<T> getInputSerializer(int inputNum, ClassLoader cl) {
		return getTypeSerializerFactory(INPUT_TYPE_SERIALIZER_FACTORY_PREFIX + inputNum,
			INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX + inputNum + SEPERATOR, cl);
	}
	
	public void setInputComparator(TypeComparatorFactory<?> factory, int inputNum) {
		setTypeComparatorFactory(factory, INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX + inputNum,
			INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPERATOR);
	}
	
	public <T> TypeComparatorFactory<T> getInputComparator(int inputNum, ClassLoader cl) {
		return getTypeComparatorFactory(INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX + inputNum,
			INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX + inputNum + SEPERATOR, cl);
	}
	
	public int getNumInputs() {
		return this.config.getInteger(NUM_INPUTS, -1);
	}
	
	public int getGroupSize(int groupIndex) {
		return this.config.getInteger(INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	
	public void addInputToGroup(int groupIndex) {
		final String grp = INPUT_GROUP_SIZE_PREFIX + groupIndex;
		this.config.setInteger(grp, this.config.getInteger(grp, 0) + 1);
		this.config.setInteger(NUM_INPUTS, this.config.getInteger(NUM_INPUTS, 0) + 1);
	}
	
	public void setInputDammed(int inputNum, boolean temp) {
		this.config.setBoolean(INPUT_DAM_PREFIX + inputNum, temp);
	}
	
	public boolean isInputDammed(int inputNum) {
		return this.config.getBoolean(INPUT_DAM_PREFIX + inputNum, false);
	}
	
	public void setInputReplayable(int inputNum, boolean persistent) {
		this.config.setBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, persistent);
	}
	
	public boolean isInputReplayable(int inputNum) {
		return this.config.getBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, false);
	}
	
	public void setInputDamReplayableMemory(int inputNum, long memory) {
		this.config.setLong(INPUT_DAM_REPLAY_MEMORY_PREFIX + inputNum, memory);
	}
	
	public long getInputDamReplayableMemory(int inputNum) {
		return this.config.getLong(INPUT_DAM_REPLAY_MEMORY_PREFIX + inputNum, -1);
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
		return this.config.getInteger(OUTPUTS_NUM, -1);
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
	
	public void setOutputSerializert(TypeSerializerFactory<?> factory) {
		setTypeSerializerFactory(factory, OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX);
	}
	
	public <T> TypeSerializerFactory<T> getOutputSerializer(ClassLoader cl) {
		return getTypeSerializerFactory(OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX, cl);
	}
	
	public void setOutputComparator(TypeComparatorFactory<?> factory, int outputNum) {
		setTypeComparatorFactory(factory, OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPERATOR);
	}
	
	public <T> TypeComparatorFactory<T> getOutputComparator(int outputNum, ClassLoader cl) {
		return getTypeComparatorFactory(OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPERATOR, cl);
	}
	
	public void setOutputDataDistribution(DataDistribution distribution) {
		this.config.setString(OUTPUT_DATA_DISTRIBUTION_CLASS, distribution.getClass().getName());
		
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);
		try {
			distribution.write(dos);
		} catch (IOException e) {
			throw new RuntimeException("Error serializing the DataDistribution: " + e.getMessage(), e);
		}

		this.config.setBytes(OUTPUT_DATA_DISTRIBUTION_STATE, baos.toByteArray());
	}
	
	public DataDistribution getOutputDataDistribution(final ClassLoader cl) throws ClassNotFoundException {
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
		
		final byte[] stateEncoded = this.config.getBytes(OUTPUT_DATA_DISTRIBUTION_STATE, null);
		if (stateEncoded == null) {
			throw new CorruptConfigurationException(
						"The configuration contained the data distribution type, but no serialized state.");
		}
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(stateEncoded);
		final DataInputStream in = new DataInputStream(bais);
		
		try {
			distribution.read(in);
			return distribution;
		} catch (Exception ex) {
			throw new RuntimeException("The deserialization of the encoded data distribution state caused an error"
				+ ex.getMessage() == null ? "." : ": " + ex.getMessage(), ex);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------

	public void setMemoryDriver(long memorySize) {
		this.config.setLong(MEMORY_DRIVER, memorySize);
	}

	public long getMemoryDriver() {
		return this.config.getLong(MEMORY_DRIVER, -1);
	}
	
	public void setMemoryInput(int inputNum, long memorySize) {
		this.config.setLong(MEMORY_INPUT_PREFIX + inputNum, memorySize);
	}

	public long getMemoryInput(int inputNum) {
		return this.config.getLong(MEMORY_INPUT_PREFIX + inputNum, -1);
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
	
	// --------------------------------------------------------------------------------------------
	//                                    Parameters for Stub Chaining
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfChainedStubs() {
		return this.config.getInteger(CHAINING_NUM_STUBS, 0);
	}
	
	public void addChainedTask(@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> chainedTaskClass, TaskConfig conf, String taskName)
	{
		int numChainedYet = this.config.getInteger(CHAINING_NUM_STUBS, 0);
		
		this.config.setString(CHAINING_TASK_PREFIX + numChainedYet, chainedTaskClass.getName());
		this.config.addAll(conf.config, CHAINING_TASKCONFIG_PREFIX + numChainedYet + SEPERATOR);
		this.config.setString(CHAINING_TASKNAME_PREFIX + numChainedYet, taskName);
		
		this.config.setInteger(CHAINING_NUM_STUBS, ++numChainedYet);
	}
	
	public TaskConfig getChainedStubConfig(int chainPos) {
		return new TaskConfig(new DelegatingConfiguration(this.config, CHAINING_TASKCONFIG_PREFIX + chainPos + SEPERATOR));
	}

	public Class<? extends ChainedDriver<?, ?>> getChainedTask(int chainPos) {
		final String className = this.config.getString(CHAINING_TASK_PREFIX + chainPos, null);
		if (className == null)
			throw new IllegalStateException("Chained Task Class missing");
		
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
	//                                    Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private final void setTypeSerializerFactory(TypeSerializerFactory<?> factory, 
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
	
	private final <T> TypeSerializerFactory<T> getTypeSerializerFactory(String classNameKey, String parametersPrefix, ClassLoader cl)
	{
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
	
	private final void setTypeComparatorFactory(TypeComparatorFactory<?> factory, 
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
	
	private final <T> TypeComparatorFactory<T> getTypeComparatorFactory(String classNameKey, String parametersPrefix, ClassLoader cl)
	{
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
	
	// --------------------------------------------------------------------------------------------
	//                          Utility class for nested Configurations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A configuration that manages a subset of keys with a common prefix from a given configuration.
	 */
	public static final class DelegatingConfiguration extends Configuration
	{
		private final Configuration backingConfig;		// the configuration actually storing the data
		
		private String prefix;							// the prefix key by which keys for this config are marked
		
		// --------------------------------------------------------------------------------------------
		
		/**
		 * Default constructor for serialization. Creates an empty delegating configuration.
		 */
		public DelegatingConfiguration() {
			this.backingConfig = new Configuration();
			this.prefix = "";
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
		public void setString(String key, String value) {
			this.backingConfig.setString(this.prefix + key, value);
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
		public byte[] getBytes(final String key, final byte[] defaultValue) {
			return this.backingConfig.getBytes(this.prefix + key, defaultValue);
		}
		
		@Override
		public void setBytes(final String key, final byte[] bytes) {
			this.backingConfig.setBytes(this.prefix + key, bytes);
		}
		
		@Override
		public void addAll(Configuration other, String prefix) {
			this.backingConfig.addAll(other, this.prefix + prefix);
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
		public int hashCode() {
			return this.prefix.hashCode() ^ this.backingConfig.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof DelegatingConfiguration) {
				DelegatingConfiguration other = (DelegatingConfiguration) obj;
				return this.prefix.equals(other.prefix) && this.backingConfig.equals(other.backingConfig);
			}
			else return false;
		}
	}
}
