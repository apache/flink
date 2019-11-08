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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilder;

/**
 * A ToRelContext impl that takes the context variables
 * used for sql expression transformation.
 */
public interface FlinkToRelContext extends RelOptTable.ToRelContext {

	/**
	 * Creates a new instance of {@link SqlExprToRexConverter} to convert sql statements
	 * to {@link org.apache.calcite.rex.RexNode}.
	 *
	 * <p>See {@link org.apache.flink.table.planner.plan.schema.FlinkRelOptTable#toRel}
	 * for details.
	 */
	SqlExprToRexConverter createSqlExprToRexConverter(RelDataType tableRowType);

	/**
	 * Creates a new instance of {@link RelBuilder} to build relational expressions.
	 */
	RelBuilder createRelBuilder();
}
