/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators;

import java.util.List;

import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Visitable;

/**
* Abstract base class for all Parallelization Contracts (PACTs).
* A Pact receives one or multiple input sets of records (see {@link Record}). It partitions and combines them
* into independent sets which are processed by user functions.
*/
public abstract class Operator implements Visitable<Operator> {
	
	protected final Configuration parameters;			// the parameters to parameterize the UDF
	
	protected CompilerHints compilerHints;				// hints to the compiler
	
	protected String name;								// the name of the contract instance. optional.
		
	private int degreeOfParallelism = -1;				// the number of parallel instances to use. -1, if unknown

	// --------------------------------------------------------------------------------------------	

	/**
	 * Creates a new contract with the given name. The parameters are empty by default and
	 * the compiler hints are not set.
	 * 
	 * @param name The name that is used to describe the contract.
	 */
	protected Operator(String name) {
		this.name = (name == null) ? "(null)" : name;
		this.parameters = new Configuration();
		this.compilerHints = new CompilerHints();
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
	 * Gets the degree of parallelism for this contract instance. The degree of parallelism denotes
	 * how many parallel instances of the user function will be spawned during the execution. If this
	 * value is <code>-1</code>, then the system will decide the number of parallel instances by itself.
	 * 
	 * @return The degree of parallelism.
	 */
	public int getDegreeOfParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Sets the degree of parallelism for this contract instance. The degree of parallelism denotes
	 * how many parallel instances of the user function will be spawned during the execution. Set this
	 * value to <code>-1</code> to let the system decide on its own.
	 * 
	 * @param degree The number of parallel instances to spawn. -1, if unspecified.
	 */
	public void setDegreeOfParallelism(int degree) {
		this.degreeOfParallelism = degree;
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
	
	/**
	 * Takes a list of operators and creates a cascade of unions of this inputs, if needed.
	 * If not needed (there was only one operator in the list), then that operator is returned.
	 * 
	 * @param operators The operators.
	 * @return The single operator or a cascade of unions of the operators.
	 */
	public static Operator createUnionCascade(List<Operator> operators) {
		return createUnionCascade((Operator[]) operators.toArray(new Operator[operators.size()]));
	}
	
	/**
	 * Takes a list of operators and creates a cascade of unions of this inputs, if needed.
	 * If not needed (there was only one operator in the list), then that operator is returned.
	 * 
	 * @param operators The operators.
	 * @return The single operator or a cascade of unions of the operators.
	 */
	public static Operator createUnionCascade(Operator... operators) {
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
	public static Operator createUnionCascade(Operator input1, Operator... input2) {
		// return cases where we don't need a union
		if (input2 == null || input2.length == 0) {
			return input1;
		} else if (input2.length == 1 && input1 == null) {
			return input2[0];
		}
		// Otherwise construct union cascade
		Union lastUnion = new Union();
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
			Union tmpUnion = new Union();
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
