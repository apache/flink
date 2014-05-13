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

package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;


/**
 * @see GenericCrosser
 */
public class CrossOperatorBase<IN1, IN2, OUT, FT extends GenericCrosser<?, ?, ?>> extends DualInputOperator<IN1, IN2, OUT, FT> {
	
	public CrossOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public CrossOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public CrossOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		this(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Marker interface to declare the second input as the smaller one.
	 */
	public static interface CrossWithSmall {}
	
	/**
	 * Marker interface to declare the second input as the larger one.
	 */
	public static interface CrossWithLarge {}
}
