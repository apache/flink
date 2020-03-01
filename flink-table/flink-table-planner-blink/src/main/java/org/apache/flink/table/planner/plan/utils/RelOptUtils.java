/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.utils;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>RelOptUtils</code> defines static utility methods for use in optimizing
 * {@link RelNode}s.
 *
 * <p>This is an extension of {@link org.apache.calcite.plan.RelOptUtil}.
 */
public class RelOptUtils {
	/**
	 * Creates a projection which casts a rel's output to a desired row type.
	 *
	 * <p>This method is inspired by {@link RelOptUtil#createCastRel}, different with that,
	 * we do not generate another {@link Project} if the {@code rel} is already a {@link Project}.
	 *
	 * @param rel Producer of rows to be converted
	 * @param castRowType Row type after cast
	 * @return Conversion rel with castRowType
	 */
	public static RelNode createCastRel(RelNode rel, RelDataType castRowType) {
		RelFactories.ProjectFactory projectFactory = RelFactories.DEFAULT_PROJECT_FACTORY;
		final RelDataType oriRowType = rel.getRowType();
		if (RelOptUtil.areRowTypesEqual(oriRowType, castRowType, true)) {
			// nothing to do
			return rel;
		}
		final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

		final List<RelDataTypeField> fieldList = oriRowType.getFieldList();
		int n = fieldList.size();
		assert n == castRowType.getFieldCount()
			: "field count: lhs [" + castRowType + "] rhs [" + oriRowType + "]";

		final List<RexNode> rhsExps;
		final RelNode input;
		if (rel instanceof Project) {
			rhsExps = ((Project) rel).getProjects();
			// Avoid to generate redundant project node.
			input = rel.getInput(0);
		} else {
			rhsExps = new ArrayList<>();
			for (RelDataTypeField field : fieldList) {
				rhsExps.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
			}
			input = rel;
		}

		final List<RexNode> castExps =
				RexUtil.generateCastExpressions(rexBuilder, castRowType, rhsExps);
		// Use names and types from castRowType.
		return projectFactory.createProject(input, castExps,
				castRowType.getFieldNames());
	}
}
