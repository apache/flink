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

package org.apache.flink.compiler.util;

import org.apache.flink.api.common.operators.RecordOperator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.types.Key;
import org.apache.flink.types.TypeInformation;


public class NoOpUnaryUdfOp<OUT> extends SingleInputOperator<OUT, OUT, NoOpFunction> implements RecordOperator {
	
	@SuppressWarnings("rawtypes")
	public static final NoOpUnaryUdfOp INSTANCE = new NoOpUnaryUdfOp();
	
	private NoOpUnaryUdfOp() {
		// pass null here because we override getOutputType to return type
		// of input operator
		super(new UserCodeClassWrapper<NoOpFunction>(NoOpFunction.class), null, "");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<? extends Key<?>>[] getKeyClasses() {
		return (Class<? extends Key<?>>[]) new Class[0];
	}

	@Override
	public UnaryOperatorInformation<OUT, OUT> getOperatorInfo() {
		TypeInformation<OUT> previousOut = input.getOperatorInfo().getOutputType();
		return new UnaryOperatorInformation<OUT, OUT>(previousOut, previousOut);
	}
}

