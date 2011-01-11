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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.Annotation;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Visitable;
import eu.stratosphere.pact.common.stub.Stub;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public abstract class Contract implements Visitable<Contract> {

	protected final String name; // the name of the contract instance. optional.

	protected final Configuration stubParameters; // the parameters that allow to parameterize the stub function

	protected final CompilerHints compilerHints; // hints to the pact compiler

	private int degreeOfParallelism = -1; // the number of parallel instances to use. -1, if unknown

	protected Class<? extends Annotation> ocClazz;

	/**
	 * Creates a new contract with the given name. The parameters are empty by default and
	 * the compiler hints are not set.
	 * 
	 * @param name
	 *        The name that is used to describe the contract.
	 */
	public Contract(String name) {
		this.name = name;

		this.stubParameters = new Configuration();
		this.compilerHints = new CompilerHints();
	}

	/**
	 * Gets the name of the contract instance. The name is only used to describe the contract instance
	 * in logging output and graphical representations.
	 * 
	 * @return The contract instance's name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the compiler hints for this contract instance. In the compiler hints, different fields may
	 * be set (for example the selectivity) that will be evaluated by the pact compiler when generating
	 * plan alternatives.
	 * 
	 * @return The compiler hints object.
	 */
	public CompilerHints getCompilerHints() {
		return compilerHints;
	}

	/**
	 * Gets the stub parameters of this contract. The stub parameters are a map that maps string keys to
	 * string or integer values. The map is accessible by the user code at runtime. Parameters that the
	 * user code needs to access at runtime to configure its behavior are typically stored in that configuration
	 * object.
	 * 
	 * @return The configuration containing the stub parameters.
	 */
	public Configuration getStubParameters() {
		return stubParameters;
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getStubParameters()
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setStubParameter(String key, String value) {
		stubParameters.setString(key, value);
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getStubParameters()
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setStubParameter(String key, int value) {
		stubParameters.setInteger(key, value);
	}

	/**
	 * Sets a stub parameters in the configuration of this contract. The stub parameters are accessible by the user
	 * code at runtime. Parameters that the user code needs to access at runtime to configure its behavior are
	 * typically stored as stub parameters.
	 * 
	 * @see #getStubParameters()
	 * @param key
	 *        The parameter key.
	 * @param value
	 *        The parameter value.
	 */
	public void setStubParameter(String key, boolean value) {
		stubParameters.setBoolean(key, value);
	}

	/**
	 * Gets the degree of parallelism for this contract instance. The degree of parallelism denotes
	 * how many parallel instances of the user function will be spawned during the execution. If this
	 * value is <code>-1</code>, then the system will decide the number of parallel instances by itself.
	 * 
	 * @return The degree of parallelism.
	 */
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}

	/**
	 * Sets the degree of parallelism for this contract instance. The degree of parallelism denotes
	 * how many parallel instances of the user function will be spawned during the execution. Set this
	 * value to <code>-1</code> to let the system decide on its own.
	 * 
	 * @param degree
	 *        The number of parallel instances to spawn. -1, if unspecified.
	 */
	public void setDegreeOfParallelism(int degree) {
		this.degreeOfParallelism = degree;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the stub that is wrapped by this contract. The stub is the actual implementation of the
	 * user code.
	 * 
	 * @return The stub of this contract.
	 */
	public abstract Class<? extends Stub<?, ?>> getStubClass();
}
