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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * The function definition of an user-defined table aggregate function.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 */
@PublicEvolving
public final class TableAggregateFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final TableAggregateFunction<?, ?> aggregateFunction;
	private final TypeInformation<?> resultTypeInfo;
	private final TypeInformation<?> accumulatorTypeInfo;

	public TableAggregateFunctionDefinition(
			String name,
			TableAggregateFunction<?, ?> aggregateFunction,
			TypeInformation<?> resultTypeInfo,
			TypeInformation<?> accTypeInfo) {
		this.name = Preconditions.checkNotNull(name);
		this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
		this.resultTypeInfo = Preconditions.checkNotNull(resultTypeInfo);
		this.accumulatorTypeInfo = Preconditions.checkNotNull(accTypeInfo);
	}

	public String getName() {
		return name;
	}

	public TableAggregateFunction<?, ?> getTableAggregateFunction() {
		return aggregateFunction;
	}

	public TypeInformation<?> getResultTypeInfo() {
		return resultTypeInfo;
	}

	public TypeInformation<?> getAccumulatorTypeInfo() {
		return accumulatorTypeInfo;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.TABLE_AGGREGATE;
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return aggregateFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return aggregateFunction.isDeterministic();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableAggregateFunctionDefinition that = (TableAggregateFunctionDefinition) o;
		return name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return name;
	}
}
