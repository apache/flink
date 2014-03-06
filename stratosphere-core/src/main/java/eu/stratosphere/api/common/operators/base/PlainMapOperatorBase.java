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

import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;


public class PlainMapOperatorBase<T extends GenericMap<?, ?>> extends SingleInputOperator<T> {
	
	public PlainMapOperatorBase(UserCodeWrapper<T> udf, String name) {
		super(udf, name);
	}
	
	public PlainMapOperatorBase(T udf, String name) {
		super(new UserCodeObjectWrapper<T>(udf), name);
	}
	
	public PlainMapOperatorBase(Class<? extends T> udf, String name) {
		super(new UserCodeClassWrapper<T>(udf), name);
	}
}
