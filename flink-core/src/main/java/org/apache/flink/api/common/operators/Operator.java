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


package org.apache.flink.api.common.operators;

import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Visitable;

/**
* Abstract base class for all operators. An operator is a source, sink, or it applies an operation to
* one or more inputs, producing a result.
 *
 * @param <OUT> Output type of the records output by this operator
*/
@Internal
public abstract class Operator<OUT> implements Visitable<Operator<?>> {
	
	protected final Configuration parameters;			// the parameters to parameterize the UDF
	
	protected CompilerHints compilerHints;				// hints to the compiler
	
	protected String name;								// the name of the contract instance. optional.
		
	private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;  // the number of parallel instances to use

	private ResourceSpec minResource;			// the minimum resource of the contract instance.

	private ResourceSpec preferredResource;	// the preferred resource of the contract instance.

	/**
	 * The return type of the user function.
	 */
	protected final OperatorInformation<OUT> operatorInfo;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new contract with the given name. The parameters are empty by default and
	 * the compiler hints are not set.
	 * 
	 * @param name The name that is used to describe the contract.
	 */
	protected Operator(OperatorInformation<OUT> operatorInfo, String name) {
		this.name = (name == null) ? "(null)" : name;
		this.parameters = new Configuration();
		this.compilerHints = new CompilerHints();
		this.operatorInfo = operatorInfo;
	}

	/**
	 * Gets the information about the operators input/output types.
	 */
	public OperatorInformation<OUT> getOperatorInfo() {
		return operatorInfo;
	}

	/**
	 * Gets the name of the contract instance. The name is only used to describe the contract instance
	 * in logging output and graphical representations.
	 * 
	 * @return The contract instance's name.
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * Sets the name of the contract instance. The name is only used to describe the contract instance
	 * in logging output and graphical representations.
	 * 
	 * @param name The operator's name.
	 */
	public void setName(String name) {
		this.name = name;
	}	

	/**
	 * Gets the compiler hints for this contract instance. In the compiler hints, different fields may
	 * be set (for example the selectivity) that will be evaluated by the pact compiler when generating
	 * plan alternatives.
	 * 
	 * @return The compiler hints object.
	 */
	public CompilerHints getCompilerHints() {
		return this.compilerHints;
	}

	/**
	 * Gets the stub parameters of this contract. The stub parameters are a map that maps string keys to
	 * string or integer values. The map is accessible by the user code at runtime. Parameters that the
	 * user code needs to access at runtime to configure its behavior are typically stored in that configuration
	 * object.
	 * 
	 * @return The configuration containing the stub parameters.
	 */
	public Configuration getParameters() {
		return this.parameters;
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getParameters()
	 * 
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setParameter(String key, String value) {
		this.parameters.setString(key, value);
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getParameters()
	 * 
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setParameter(String key, int value) {
		this.parameters.setInteger(key, value);
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getParameters()
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setParameter(String key, boolean value) {
		this.parameters.setBoolean(key, value);
	}

	/**
	 * Gets the parallelism for this contract instance. The parallelism denotes how many
	 * parallel instances of the user function will be spawned during the execution. If this
	 * value is {@link ExecutionConfig#PARALLELISM_DEFAULT}, then the system will decide the
	 * number of parallel instances by itself.
	 *
	 * @return The parallelism.
	 */
	public int getParallelism() {
		return this.parallelism;
	}

	/**
	 * Sets the parallelism for this contract instance. The parallelism denotes
	 * how many parallel instances of the user function will be spawned during the execution.
	 *
	 * @param parallelism The number of parallel instances to spawn. Set this value to
	 *        {@link ExecutionConfig#PARALLELISM_DEFAULT} to let the system decide on its own.
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * Gets the minimum resource for this contract instance. The minimum resource denotes how many
	 * resources will be needed in the minimum for the user function during the execution.
	 *
	 * @return The minimum resource of this operator.
	 */
	public ResourceSpec getMinResource() {
		return this.minResource;
	}

	/**
	 * Gets the preferred resource for this contract instance. The preferred resource denotes how many
	 * resources will be needed in the maximum for the user function during the execution.
	 *
	 * @return The preferred resource of this operator.
	 */
	public ResourceSpec getPreferredResource() {
		return this.preferredResource;
	}

	/**
	 * Sets the minimum and preferred resources for this contract instance. The resource denotes
	 * how many memories and cpu cores of the user function will be consumed during the execution.
	 *
	 * @param minResource The minimum resource of this operator.
	 * @param preferredResource The preferred resource of this operator.
	 */
	public void setResource(ResourceSpec minResource, ResourceSpec preferredResource) {
		this.minResource = minResource;
		this.preferredResource = preferredResource;
	}
	
	
	/**
	 * Gets the user code wrapper. In the case of a pact, that object will be the stub with the user function,
	 * in the case of an input or output format, it will be the format object.  
	 * 
	 * @return The class with the user code.
	 */
	public UserCodeWrapper<?> getUserCodeWrapper() {
		return null;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Takes a list of operators and creates a cascade of unions of this inputs, if needed.
	 * If not needed (there was only one operator in the list), then that operator is returned.
	 * 
	 * @param operators The operators.
	 * @return The single operator or a cascade of unions of the operators.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Operator<T> createUnionCascade(List<? extends Operator<T>> operators) {
		return createUnionCascade((Operator<T>[]) operators.toArray(new Operator[operators.size()]));
	}
	
	/**
	 * Takes a list of operators and creates a cascade of unions of this inputs, if needed.
	 * If not needed (there was only one operator in the list), then that operator is returned.
	 * 
	 * @param operators The operators.
	 * @return The single operator or a cascade of unions of the operators.
	 */
	public static <T> Operator<T> createUnionCascade(Operator<T>... operators) {
		return createUnionCascade(null, operators);
	}
	
	/**
	 * Takes a single Operator and a list of operators and creates a cascade of unions of this inputs, if needed.
	 * If not needed there was only one operator as input, then this operator is returned.
	 * 
	 * @param input1 The first input operator.
	 * @param input2 The other input operators.
	 * 
	 * @return The single operator or a cascade of unions of the operators.
	 */
	public static <T> Operator<T> createUnionCascade(Operator<T> input1, Operator<T>... input2) {
		// return cases where we don't need a union
		if (input2 == null || input2.length == 0) {
			return input1;
		} else if (input2.length == 1 && input1 == null) {
			return input2[0];
		}

		TypeInformation<T> type = null;
		if (input1 != null) {
			type = input1.getOperatorInfo().getOutputType();
		} else if (input2.length > 0 && input2[0] != null) {
			type = input2[0].getOperatorInfo().getOutputType();
		} else {
			throw new IllegalArgumentException("Could not determine type information from inputs.");
		}

		// Otherwise construct union cascade
		Union<T> lastUnion = new Union<T>(new BinaryOperatorInformation<T, T, T>(type, type, type), "<unknown>");

		int i;
		if (input2[0] == null) {
			throw new IllegalArgumentException("The input may not contain null elements.");
		}
		lastUnion.setFirstInput(input2[0]);

		if (input1 != null) {
			lastUnion.setSecondInput(input1);
			i = 1;
		} else {
			if (input2[1] == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			}
			lastUnion.setSecondInput(input2[1]);
			i = 2;
		}
		for (; i < input2.length; i++) {
			Union<T> tmpUnion = new Union<T>(new BinaryOperatorInformation<T, T, T>(type, type, type), "<unknown>");
			tmpUnion.setSecondInput(lastUnion);
			if (input2[i] == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			}
			tmpUnion.setFirstInput(input2[i]);
			lastUnion = tmpUnion;
		}
		return lastUnion;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return getClass().getSimpleName() + " - " + getName();
	}
}
