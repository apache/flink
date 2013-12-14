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

package eu.stratosphere.api.record.operators;

import eu.stratosphere.api.operators.base.GenericCrossContract.CrossWithLarge;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.api.record.functions.CrossStub;


/**
 * This operator represents a Cartesian-Product operation. Of the two inputs, the first is expected to be large
 * and the second is expected to be small. 
 * 
 * @see CrossStub
 */
public class CrossWithLargeOperator extends CrossOperator implements CrossWithLarge {
	
	/**
	 * Creates a Builder with the provided {@link CrossStub} implementation.
	 * 
	 * @param udf The {@link CrossStub} implementation for this Cross contract.
	 */
	public static Builder builder(CrossStub udf) {
		return new Builder(new UserCodeObjectWrapper<CrossStub>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link CrossStub} implementation.
	 * 
	 * @param udf The {@link CrossStub} implementation for this Cross contract.
	 */
	public static Builder builder(Class<? extends CrossStub> udf) {
		return new Builder(new UserCodeClassWrapper<CrossStub>(udf));
	}
	
	/**
	 * The private constructor that only gets invoked from the Builder.
	 * @param builder
	 */
	protected CrossWithLargeOperator(Builder builder) {
		super(builder);
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Builder pattern, straight from Joshua Bloch's Effective Java (2nd Edition).
	 */
	public static class Builder extends CrossOperator.Builder {
		
		/**
		 * Creates a Builder with the provided {@link CrossStub} implementation.
		 * 
		 * @param udf The {@link CrossStub} implementation for this Cross contract.
		 */
		private Builder(UserCodeWrapper<CrossStub> udf) {
			super(udf);
		}
		
		/**
		 * Creates and returns a CrossOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created contract
		 */
		public CrossWithLargeOperator build() {
			return new CrossWithLargeOperator(this);
		}
	}
}
