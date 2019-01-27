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

package org.apache.flink.sql.parser.plan;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * Blink sql validator.
 */
public class FlinkSqlValidator extends SqlValidatorImpl {

	public FlinkSqlValidator(
		SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
		RelDataTypeFactory typeFactory) {
		super(opTab, catalogReader, typeFactory, SqlConformanceEnum.DEFAULT);
	}

	@Override
	protected RelDataType getLogicalSourceRowType(RelDataType sourceRowType, SqlInsert insert) {
		return ((JavaTypeFactory) typeFactory).toSql(sourceRowType);
	}

	@Override
	protected RelDataType getLogicalTargetRowType(RelDataType targetRowType, SqlInsert insert) {
		return ((JavaTypeFactory) typeFactory).toSql(targetRowType);
	}

	public void validateSCNamespace(SqlValidatorNamespace namespace) {
		namespace.validate(unknownType);
		if (namespace.getNode() != null) {
			setValidatedNodeType(namespace.getNode(), namespace.getType());
		}
	}

	public SqlNode registerViewQuery(SqlNode topNode) {

		try {
			Class<?> emptyScopeClazz = Class.forName("org.apache.calcite.sql.validate.EmptyScope");
			Constructor emptyScopeConstructor = emptyScopeClazz.getDeclaredConstructor(SqlValidatorImpl.class);
			emptyScopeConstructor.setAccessible(true);
			SqlValidatorScope scope = (SqlValidatorScope) emptyScopeConstructor.newInstance(this);

			Class<?> catalogScope = Class.forName("org.apache.calcite.sql.validate.CatalogScope");
			Constructor catalogScopeConstructor =
				catalogScope.getDeclaredConstructor(SqlValidatorScope.class, List.class);
			catalogScopeConstructor.setAccessible(true);
			scope = (SqlValidatorScope) catalogScopeConstructor.newInstance(scope, ImmutableList.of("CATALOG"));

			Class<?> clazz = this.getClass();
			Class<?> superClazz = clazz.getSuperclass();
			Field cursorSetField = superClazz.getDeclaredField("cursorSet");
			cursorSetField.setAccessible(true);
			Set<SqlNode> cursorSet = (Set<SqlNode>) cursorSetField.get(this);
			SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
			cursorSet.add(outermostNode);

			Field topField = superClazz.getDeclaredField("top");
			topField.setAccessible(true);
			topField.set(this, outermostNode);

			Class<?> sqlValidatorClazz = SqlValidatorImpl.class;
			Method registerQueryMethod = sqlValidatorClazz.getDeclaredMethod(
				"registerQuery",
				SqlValidatorScope.class,
				SqlValidatorScope.class,
				SqlNode.class,
				SqlNode.class,
				String.class,
				boolean.class);
			registerQueryMethod.setAccessible(true);

			TRACER.trace("After unconditional rewrite: " + outermostNode.toString());
			if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
				registerQueryMethod.invoke(this, scope, null, outermostNode, outermostNode, null, false);
			}

			return outermostNode;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("registerViewQuery failed! " + e.getMessage());
		}
	}
}
