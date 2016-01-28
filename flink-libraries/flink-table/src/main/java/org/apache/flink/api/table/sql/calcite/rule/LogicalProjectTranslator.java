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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sql.calcite.DataSetRelNodeRule;
import org.apache.flink.api.table.sql.calcite.FlinkUtils;
import org.apache.flink.api.table.sql.calcite.RexToExpr;
import org.apache.flink.api.table.sql.calcite.flinkFunction.ProjectFunction;
import org.apache.flink.api.table.sql.calcite.node.DataSetMap;
import org.apache.flink.api.table.expressions.Expression;
import org.apache.flink.api.table.expressions.Naming;
import org.apache.flink.api.table.plan.TypeConverter;
import org.apache.flink.api.table.sql.calcite.DataSetRelNodeRule;
import org.apache.flink.api.table.sql.calcite.FlinkUtils;
import org.apache.flink.api.table.sql.calcite.RexToExpr;
import org.apache.flink.api.table.typeinfo.RowTypeInfo;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.LinkedList;
import java.util.List;

public class LogicalProjectTranslator extends DataSetRelNodeRule {
	
	public LogicalProjectTranslator(boolean exprCodeGen) {
		super(LogicalProject.class, Convention.NONE, "FlinkProjectRule", exprCodeGen);
	}
	
	@Override
	public RelNode convert(RelNode rel) {
		LogicalProject project = (LogicalProject) rel;
		RelNode input = project.getInput();
		RelDataType inputRowType = input.getRowType();
		List<RexNode> projects = project.getProjects();
		RelDataType rowType = project.getRowType();
		
		RexProgram projectProgram = RexProgram.create(
			inputRowType,
			projects,
			null,
			rowType,
			project.getCluster().getRexBuilder());
		
		List<Naming> projectExprs = new LinkedList<>();
		
		if (!projectProgram.projectsOnlyIdentity()) {
			
			List<Tuple2<String, TypeInformation<?>>> inputFieldsInfo = new LinkedList<>();
			for (RelDataTypeField relField : inputRowType.getFieldList()) {
				String renamedFieldName = relField.getName();
				RelDataType sqlType = relField.getType();
				TypeInformation<?> typeInformation = TypeConverter.sqlTypeToTypeInfo(sqlType.getSqlTypeName());
				inputFieldsInfo.add(new Tuple2<String, TypeInformation<?>>(renamedFieldName, typeInformation));
			}
			
			// translate project RexNode to Flink expression.
			List<Pair<RexLocalRef, String>> namedProjects = projectProgram.getNamedProjects();
			for (Pair<RexLocalRef, String> rexAndName : namedProjects) {
				RexNode rexNode = projectProgram.expandLocalRef(rexAndName.getKey());
				Expression projectExpr = RexToExpr.translate(rexNode, JavaConversions.asScalaBuffer(inputFieldsInfo).toSeq());
				projectExprs.add(new Naming(projectExpr, rexAndName.getValue()));
			}
		}
		
		final RowTypeInfo outputType = FlinkUtils.getRowTypeInfoFromRelDataType(rowType);
		
		RichMapFunction<Row, Row> mapFunction = null;
		if (isExprCodeGen()) {
			// TODO generate expression code.
		} else {
			if (projectExprs.isEmpty()) {
				// no project, just rename, translate to RenameOperator while build Flink program, 
				// MapFunction is not required.
			} else {
				mapFunction = new ProjectFunction(outputType, projectExprs);
			}
		}
		
		return new DataSetMap<>(rel.getCluster(), rel.getTraitSet(), input, outputType, mapFunction);
	}
}
