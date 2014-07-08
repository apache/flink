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

import eu.stratosphere.api.common.functions.GenericFlatMap;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;


/**
 * @see GenericFlatMap
 */
public class FilterOperatorBase<T, FT extends GenericFlatMap<T, T>> extends SingleInputOperator<T, T, FT> {
	
	public FilterOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public FilterOperatorBase(FT udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public FilterOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
}
