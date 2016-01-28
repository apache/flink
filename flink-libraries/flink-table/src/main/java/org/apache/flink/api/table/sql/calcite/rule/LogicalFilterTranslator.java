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
package org.apache.flink.api.table.sql.calcite.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.sql.calcite.DataSetRelNodeRule;
import org.apache.flink.api.table.sql.calcite.FlinkUtils;
import org.apache.flink.api.table.sql.calcite.RexToExpr;
import org.apache.flink.api.table.sql.calcite.flinkFunction.FilterFunction;
import org.apache.flink.api.table.sql.calcite.node.DataSetMapPartition;
import org.apache.flink.api.table.expressions.Expression;
import org.apache.flink.api.table.plan.TypeConverter;
import org.apache.flink.api.table.sql.calcite.FlinkUtils;
import org.apache.flink.api.table.sql.calcite.RexToExpr;
import org.apache.flink.api.table.typeinfo.RowTypeInfo;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.LinkedList;
import java.util.List;

public class LogicalFilterTranslator extends DataSetRelNodeRule {
	public LogicalFilterTranslator(boolean exprCodeGen) {
		super(LogicalFilter.class, Convention.NONE, "FlinkFilterRule2", exprCodeGen);
	}
	
	@Override
	public RelNode convert(RelNode rel) {
		LogicalFilter filterRelNode = (LogicalFilter) rel;
		RelNode input = filterRelNode.getInput();
		RelDataType inputRowType = input.getRowType();
		RelDataType rowType = filterRelNode.getRowType();
		
		// build program
		RexBuilder rexBuilder = filterRelNode.getCluster().getRexBuilder();
		RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
		programBuilder.addIdentity();
		programBuilder.addCondition(filterRelNode.getCondition());
		RexProgram program = programBuilder.getProgram();
		
		RexNode rexCondition = program.expandLocalRef(program.getCondition());
		List<Tuple2<String, TypeInformation<?>>> inputFieldsInfo = new LinkedList<>();
		for (RelDataTypeField relField : inputRowType.getFieldList()) {
			String renamedFieldName = relField.getName();
			RelDataType sqlType = relField.getType();
			TypeInformation<?> typeInformation = TypeConverter.sqlTypeToTypeInfo(sqlType.getSqlTypeName());
			inputFieldsInfo.add(new Tuple2<String, TypeInformation<?>>(renamedFieldName, typeInformation));
		}
		// translate to flink expression.
		Expression filterExpression = RexToExpr.translate(rexCondition,
			JavaConversions.asScalaBuffer(inputFieldsInfo).toSeq());
		
		final RowTypeInfo outputType = FlinkUtils.getRowTypeInfoFromRelDataType(rowType);
		
		FilterFunction filterFunction = null;
		if (isExprCodeGen()) {
			//TODO generate expression code.
		} else {
			filterFunction = new FilterFunction(outputType, filterExpression);
		}
		
		return new DataSetMapPartition<>(rel.getCluster(), rel.getTraitSet(), input, outputType, filterFunction);
	}
}
