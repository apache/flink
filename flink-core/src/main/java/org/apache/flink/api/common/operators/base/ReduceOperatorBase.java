/**
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


package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;


/**
 * Base data flow operator for Reduce user-defined functions. Accepts reduce functions
 * and key positions. The key positions are expected in the flattened common data model.
 * 
 * @see org.apache.flink.api.common.functions.ReduceFunction
 *
 * @param <T> The type (parameters and return type) of the reduce function.
 * @param <FT> The type of the reduce function.
 */
public class ReduceOperatorBase<T, FT extends ReduceFunction<T>> extends SingleInputOperator<T, T, FT> {

	/**
	 * Creates a grouped reduce data flow operator.
	 * 
	 * @param udf The user-defined function, contained in the UserCodeWrapper.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param keyPositions The positions of the key fields, in the common data model (flattened).
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<T, T> operatorInfo, int[] keyPositions, String name) {
		super(udf, operatorInfo, keyPositions, name);
	}
	
	/**
	 * Creates a grouped reduce data flow operator.
	 * 
	 * @param udf The user-defined function, as a function object.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param keyPositions The positions of the key fields, in the common data model (flattened).
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(FT udf, UnaryOperatorInformation<T, T> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	/**
	 * Creates a grouped reduce data flow operator.
	 * 
	 * @param udf The class representing the parameterless user-defined function.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param keyPositions The positions of the key fields, in the common data model (flattened).
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<T, T> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Non-grouped reduce operations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a non-grouped reduce data flow operator (all-reduce).
	 * 
	 * @param udf The user-defined function, contained in the UserCodeWrapper.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	/**
	 * Creates a non-grouped reduce data flow operator (all-reduce).
	 * 
	 * @param udf The user-defined function, as a function object.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(FT udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	/**
	 * Creates a non-grouped reduce data flow operator (all-reduce).
	 * 
	 * @param udf The class representing the parameterless user-defined function.
	 * @param operatorInfo The type information, describing input and output types of the reduce function.
	 * @param name The name of the operator (for logging and messages).
	 */
	public ReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
}
