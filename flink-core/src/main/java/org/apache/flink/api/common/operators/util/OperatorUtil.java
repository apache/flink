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


package org.apache.flink.api.common.operators.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GenericCollectorMap;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.base.CollectorMapOperatorBase;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.GenericDataSinkBase;
import org.apache.flink.api.common.operators.base.GenericDataSourceBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;

/**
 * Convenience methods when dealing with {@link Operator}s.
 */
@SuppressWarnings("deprecation")
public class OperatorUtil {
	
	@SuppressWarnings("rawtypes")
	private final static Map<Class<?>, Class<? extends Operator>> STUB_CONTRACTS =
		new LinkedHashMap<Class<?>, Class<? extends Operator>>();

	static {
		STUB_CONTRACTS.put(GenericCollectorMap.class, CollectorMapOperatorBase.class);
		STUB_CONTRACTS.put(GroupReduceFunction.class, GroupReduceOperatorBase.class);
		STUB_CONTRACTS.put(CoGroupFunction.class, CoGroupOperatorBase.class);
		STUB_CONTRACTS.put(CrossFunction.class, CrossOperatorBase.class);
		STUB_CONTRACTS.put(FlatJoinFunction.class, JoinOperatorBase.class);
		STUB_CONTRACTS.put(FileInputFormat.class, GenericDataSourceBase.class);
		STUB_CONTRACTS.put(FileOutputFormat.class, GenericDataSinkBase.class);
		STUB_CONTRACTS.put(InputFormat.class, GenericDataSourceBase.class);
		STUB_CONTRACTS.put(OutputFormat.class, GenericDataSinkBase.class);
	}

	/**
	 * Returns the associated {@link Operator} type for the given {@link org.apache.flink.api.common.functions.RichFunction} class.
	 * 
	 * @param stubClass
	 *        the stub class
	 * @return the associated Operator type
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends Operator> getContractClass(final Class<?> stubClass) {
		if (stubClass == null) {
			return null;
		}
		final Class<?> contract = STUB_CONTRACTS.get(stubClass);
		if (contract != null) {
			return (Class<? extends Operator<?>>) contract;
		}
		Iterator<Entry<Class<?>, Class<? extends Operator>>> stubContracts = STUB_CONTRACTS.entrySet().iterator();
		while (stubContracts.hasNext()) {
			Map.Entry<Class<?>, Class<? extends Operator>> entry = stubContracts.next();
			if (entry.getKey().isAssignableFrom(stubClass)) {
				return entry.getValue();
			}
		}
		return null;

	}

	/**
	 * Returns the number of inputs for the given {@link Operator} type.<br>
	 * Currently, it can be 0, 1, or 2.
	 * 
	 * @param contractType
	 *        the type of the Operator
	 * @return the number of input contracts
	 */
	public static int getNumInputs(final Class<? extends Operator<?>> contractType) {

		if (GenericDataSourceBase.class.isAssignableFrom(contractType)) {
			return 0;
		}
		if (GenericDataSinkBase.class.isAssignableFrom(contractType)
			|| SingleInputOperator.class.isAssignableFrom(contractType)) {
			return 1;
		}
		if (DualInputOperator.class.isAssignableFrom(contractType)) {
			return 2;
		}
		throw new IllegalArgumentException("not supported");
	}


	/**
	 * Sets the inputs of the given {@link Operator}.<br>
	 * Currently, the list can have 0, 1, or 2 elements and the number of elements must match the type of the contract.
	 * 
	 * @param contract
	 *        the Operator whose inputs should be set
	 * @param inputs
	 *        all input contracts to this contract
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void setInputs(final Operator<?> contract, final List<List<Operator>> inputs) {
		if (contract instanceof GenericDataSinkBase) {
			if (inputs.size() != 1) {
				throw new IllegalArgumentException("wrong number of inputs");
			}
			((GenericDataSinkBase) contract).setInputs(inputs.get(0));
		} else if (contract instanceof SingleInputOperator) {
			if (inputs.size() != 1) {
				throw new IllegalArgumentException("wrong number of inputs");
			}
			((SingleInputOperator) contract).setInputs(inputs.get(0));
		} else if (contract instanceof DualInputOperator) {
			if (inputs.size() != 2) {
				throw new IllegalArgumentException("wrong number of inputs");
			}
			((DualInputOperator) contract).setFirstInputs(inputs.get(0));
			((DualInputOperator) contract).setSecondInputs(inputs.get(1));
		}
	}
}
