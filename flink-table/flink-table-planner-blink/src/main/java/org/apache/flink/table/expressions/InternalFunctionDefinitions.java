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

package org.apache.flink.table.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.SCALAR_FUNCTION;

/**
 * Dictionary of function definitions for all internal used functions.
 */
public class InternalFunctionDefinitions {

	public static final FunctionDefinition THROW_EXCEPTION = new FunctionDefinition("throwException", SCALAR_FUNCTION);

	public static final FunctionDefinition DENSE_RANK = new FunctionDefinition("DENSE_RANK", AGGREGATE_FUNCTION);

	public static final FunctionDefinition FIRST_VALUE = new FunctionDefinition("FIRST_VALUE", AGGREGATE_FUNCTION);

	public static final FunctionDefinition STDDEV = new FunctionDefinition("STDDEV", AGGREGATE_FUNCTION);

	public static final FunctionDefinition LEAD = new FunctionDefinition("LEAD", AGGREGATE_FUNCTION);

	public static final FunctionDefinition LAG = new FunctionDefinition("LAG", AGGREGATE_FUNCTION);

	public static final FunctionDefinition LAST_VALUE = new FunctionDefinition("LAST_VALUE", AGGREGATE_FUNCTION);

	public static final FunctionDefinition RANK = new FunctionDefinition("RANK", AGGREGATE_FUNCTION);

	public static final FunctionDefinition ROW_NUMBER = new FunctionDefinition("ROW_NUMBER", AGGREGATE_FUNCTION);

	public static final FunctionDefinition SINGLE_VALUE = new FunctionDefinition("SINGLE_VALUE", AGGREGATE_FUNCTION);

	public static final FunctionDefinition CONCAT_AGG = new FunctionDefinition("CONCAT_AGG", AGGREGATE_FUNCTION);

	public static final FunctionDefinition VARIANCE = new FunctionDefinition("VARIANCE", AGGREGATE_FUNCTION);

	public static List<FunctionDefinition> getDefinitions() {
		final Field[] fields = InternalFunctionDefinitions.class.getFields();
		final List<FunctionDefinition> list = new ArrayList<>(fields.length);
		for (Field field : fields) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					final FunctionDefinition funcDef = (FunctionDefinition) field.get(InternalFunctionDefinitions.class);
					list.add(Preconditions.checkNotNull(funcDef));
				} catch (IllegalAccessException e) {
					throw new TableException(
							"The function definition for field " + field.getName() + " is not accessible.", e);
				}
			}
		}
		return list;
	}
}
