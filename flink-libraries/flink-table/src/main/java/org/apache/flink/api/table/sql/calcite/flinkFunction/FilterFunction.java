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
package org.apache.flink.api.table.sql.calcite.flinkFunction;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.expressions.Expression;
import org.apache.flink.api.table.typeinfo.RowTypeInfo;
import org.apache.flink.util.Collector;

public class FilterFunction extends RichMapPartitionFunction<Row, Row> {
	
	private RowTypeInfo outputType;
	private Expression filterExpression;
	
	public FilterFunction(RowTypeInfo outputType, Expression filterExpression) {
		this.outputType = outputType;
		this.filterExpression = filterExpression;
	}
	
	@Override
	public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
		 for (Row record : values) {
			 //if (filterExpression.eval(record)), miss 'eval' support from expression.
			 out.collect(record);
		 }
	}
}
