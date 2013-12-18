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

package eu.stratosphere.api.record.operators;

import eu.stratosphere.api.operators.base.CrossOperatorBase.CrossWithSmall;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.api.record.functions.CrossFunction;


/**
 * This operator represents a Cartesian-Product operation. Of the two inputs, the first is expected to be large
 * and the second is expected to be small. 
 * 
 * @see CrossFunction
 */
public class CrossWithSmallOperator extends CrossOperator implements CrossWithSmall {
	
	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross contract.
	 */
	public static Builder builder(CrossFunction udf) {
		return new Builder(new UserCodeObjectWrapper<CrossFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross contract.
	 */
	public static Builder builder(Class<? extends CrossFunction> udf) {
		return new Builder(new UserCodeClassWrapper<CrossFunction>(udf));
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected CrossWithSmallOperator(Builder builder) {
		super(builder);
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder extends CrossOperator.Builder {
		
		/**
		 * Creates a Builder with the provided {@link CrossFunction} implementation.
		 * 
		 * @param udf The {@link CrossFunction} implementation for this Cross contract.
		 */
		private Builder(UserCodeWrapper<CrossFunction> udf) {
			super(udf);
		}
		
		/**
		 * Creates and returns a CrossOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public CrossWithSmallOperator build() {
			return new CrossWithSmallOperator(this);
		}
	}
}
