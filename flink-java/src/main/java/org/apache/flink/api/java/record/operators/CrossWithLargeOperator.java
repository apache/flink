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


package org.apache.flink.api.java.record.operators;

import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossWithLarge;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.record.functions.CrossFunction;


/**
 * This operator represents a Cartesian-Product operation. Of the two inputs, the first is expected to be large
 * and the second is expected to be small. 
 * 
 * @see CrossFunction
 */
public class CrossWithLargeOperator extends CrossOperator implements CrossWithLarge {
	
	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross operator.
	 */
	public static Builder builder(CrossFunction udf) {
		return new Builder(new UserCodeObjectWrapper<CrossFunction>(udf));
	}
	
	/**
	 * Creates a Builder with the provided {@link CrossFunction} implementation.
	 * 
	 * @param udf The {@link CrossFunction} implementation for this Cross operator.
	 */
	public static Builder builder(Class<? extends CrossFunction> udf) {
		return new Builder(new UserCodeClassWrapper<CrossFunction>(udf));
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
		 * Creates a Builder with the provided {@link CrossFunction} implementation.
		 * 
		 * @param udf The {@link CrossFunction} implementation for this Cross operator.
		 */
		private Builder(UserCodeWrapper<CrossFunction> udf) {
			super(udf);
		}
		
		/**
		 * Creates and returns a CrossOperator from using the values given 
		 * to the builder.
		 * 
		 * @return The created operator
		 */
		@Override
		public CrossWithLargeOperator build() {
			setNameIfUnset();
			return new CrossWithLargeOperator(this);
		}
	}
}
