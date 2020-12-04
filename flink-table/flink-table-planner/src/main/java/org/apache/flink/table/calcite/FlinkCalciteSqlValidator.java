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

package org.apache.flink.table.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * Extends Calcite's {@link SqlValidator} by Flink-specific behavior.
 */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

	public FlinkCalciteSqlValidator(
			SqlOperatorTable opTab,
			SqlValidatorCatalogReader catalogReader,
			RelDataTypeFactory typeFactory,
			SqlValidator.Config config) {
		super(opTab, catalogReader, typeFactory, config);
	}

	@Override
	protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
		// Due to the improper translation of lateral table left outer join in Calcite, we need to
		// temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
		if (join.getJoinType() == JoinType.LEFT &&
				SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
			final SqlNode condition = join.getCondition();
			if (condition != null &&
					(!SqlUtil.isLiteral(condition) || ((SqlLiteral) condition).getValueAs(Boolean.class) != Boolean.TRUE)) {
				throw new ValidationException(
					String.format(
						"Left outer joins with a table function do not accept a predicate such as %s. " +
						"Only literal TRUE is accepted.",
						condition));
			}
		}
		super.validateJoin(join, scope);
	}
}
