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

package org.apache.flink.sql.parser.node;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Utility for creating {@link SqlTreeNode}.
 */
public class SqlTreeNodes {

	private static final SqlDialect DUMMY = SqlDialect.DatabaseProduct.DB2.getDialect();

	public static SqlTreeNode source(SqlParserPos pos, String tableName) {
		return new SqlTableSourceNode(pos, SqlNodeType.SOURCE, tableName);
	}

	public static SqlTreeNode sink(SqlParserPos pos, SqlTreeNode input, String tableName) {
		return new SqlTableSinkNode(pos, input, tableName);
	}

	public static SqlTreeNode dim(SqlParserPos pos, String tableName) {
		return new SqlTableSourceNode(pos, SqlNodeType.DIM, tableName);
	}

	public static SqlTreeNode select(SqlParserPos pos, SqlTreeNode input, SqlNode selection) {
		return new SqlSelectNode(pos, input, toSqlString(selection));
	}

	public static SqlTreeNode filter(SqlParserPos pos, SqlTreeNode input, SqlNode condition) {
		return new SqlFilterNode(pos, input, toSqlString(condition));
	}

	public static SqlTreeNode group(SqlParserPos pos, SqlTreeNode input, SqlNode groupKeys) {
		return new SqlGroupNode(pos, input, toSqlString(groupKeys));
	}

	public static SqlTreeNode join(SqlParserPos pos, SqlTreeNode left, SqlTreeNode right, JoinType joinType) {
		return new SqlJoinNode(pos, left, right, joinType);
	}

	public static SqlTreeNode union(SqlParserPos pos, List<SqlTreeNode> nodes) {
		List<SqlTreeNode> children = new ArrayList<>();
		for (SqlTreeNode node : nodes) {
			if (node instanceof SqlUnionNode) {
				children.addAll(node.getInputs());
			} else {
				children.add(node);
			}
		}
		return new SqlUnionNode(pos, children);
	}

	public static SqlTreeNode topn(SqlParserPos pos, SqlTreeNode input, String desc) {
		return new SqlTopnNode(pos, input, desc);
	}

	public static SqlTreeNode udtf(SqlParserPos pos, SqlNode tf) {
		return new SqlTableFunctionNode(pos, toSqlString(tf));
	}

	public static SqlTreeNode correlate(SqlParserPos pos, SqlTreeNode input, String desc) {
		return new SqlCorrelateNode(pos, input, desc);
	}

	public static SqlTreeNode cep(SqlParserPos pos, SqlTreeNode input, SqlNode pattern) {
		return new SqlCepNode(pos, input, toSqlString(pattern));
	}

	protected static String toSqlString(SqlNode node) {
		return node.toSqlString(DUMMY).toString();
	}

	// =====================================================================================

	/**
	 *
	 */
	public abstract static class AbstractSqlTreeNode implements SqlTreeNode {

		protected final SqlParserPos pos;
		protected final SqlNodeType type;
		protected final String desc;

		/**
		 * Creates a node.
		 *
		 * @param pos  Parser position, must not be null.
		 * @param type sql node type
		 */
		AbstractSqlTreeNode(SqlParserPos pos, SqlNodeType type, String desc) {
			this.pos = Objects.requireNonNull(pos);
			this.type = Objects.requireNonNull(type);
			this.desc = desc;
		}

		@Override
		public String explain() {
			return desc;
		}

		@Override
		public SqlParserPos getParserPosition() {
			return pos;
		}

		@Override
		public SqlNodeType getNodeType() {
			return type;
		}

		@Override
		public SqlTreeNode getInput(int i) {
			return getInputs().get(0);
		}

	}

	/**
	 *
	 */
	public abstract static class SingleSqlTreeNode extends AbstractSqlTreeNode {

		protected SqlTreeNode input;

		SingleSqlTreeNode(SqlParserPos pos, SqlNodeType type, SqlTreeNode input, String desc) {
			super(pos, type, desc);
			this.input = input;
		}

		@Override
		public List<SqlTreeNode> getInputs() {
			return ImmutableList.of(input);
		}
	}

	/**
	 *
	 */
	public static class SqlCorrelateNode extends SingleSqlTreeNode {

		SqlCorrelateNode(SqlParserPos pos, SqlTreeNode input, String desc) {
			super(pos, SqlNodeType.UDTF, input, desc);
		}
	}

	/**
	 *
	 */
	public static class SqlFilterNode extends SingleSqlTreeNode {

		private final String condition;

		/**
		 * Creates a filter node.
		 */
		SqlFilterNode(SqlParserPos pos, SqlTreeNode input, String condition) {
			super(pos, SqlNodeType.FILTER, input, condition);
			this.condition = condition;
		}

		public String getCondition() {
			return condition;
		}
	}

	/**
	 *
	 */
	public static class SqlGroupNode extends SingleSqlTreeNode {

		private final String groupKeys;

		SqlGroupNode(SqlParserPos pos, SqlTreeNode input, String groupKeys) {
			super(pos, SqlNodeType.GROUP, input, groupKeys);
			this.groupKeys = groupKeys;
		}

		public String getGroupKeys() {
			return groupKeys;
		}
	}

	/**
	 *
	 */
	public static class SqlJoinNode extends AbstractSqlTreeNode {

		private final SqlTreeNode left;
		private final SqlTreeNode right;
		private final JoinType joinType;

		/**
		 * Creates a join node.
		 */
		SqlJoinNode(SqlParserPos pos, SqlTreeNode left, SqlTreeNode right, JoinType joinType) {
			super(pos, SqlNodeType.JOIN, "");
			this.left = left;
			this.right = right;
			this.joinType = joinType;
		}

		@Override
		public List<SqlTreeNode> getInputs() {
			return ImmutableList.of(left, right);
		}
	}

	/**
	 *
	 */
	public static class SqlSelectNode extends SingleSqlTreeNode {

		/**
		 * Creates a SELECT node.
		 */
		SqlSelectNode(SqlParserPos pos, SqlTreeNode input, String projection) {
			super(pos, SqlNodeType.SELECT, input, projection);
		}
	}

	/**
	 *
	 */
	public static class SqlTableFunctionNode extends AbstractSqlTreeNode {

		/**
		 * Creates a udtf node.
		 */
		SqlTableFunctionNode(SqlParserPos pos, String desc) {
			super(pos, SqlNodeType.OTHER, desc);
		}

		@Override
		public List<SqlTreeNode> getInputs() {
			return Collections.emptyList();
		}
	}

	/**
	 *
	 */
	public static class SqlTableSinkNode extends SingleSqlTreeNode {

		SqlTableSinkNode(SqlParserPos pos, SqlTreeNode input, String tableName) {
			super(pos, SqlNodeType.SINK, input, tableName);
		}
	}

	/**
	 *
	 */
	public static class SqlTableSourceNode extends AbstractSqlTreeNode {

		private final String tableName;

		/**
		 * Creates a node.
		 *
		 * @param pos Parser position, must not be null.
		 */
		public SqlTableSourceNode(SqlParserPos pos, SqlNodeType type, String tableName) {
			super(pos, type, tableName);
			this.tableName = tableName;
		}

		public String getTableName() {
			return tableName;
		}

		@Override
		public List<SqlTreeNode> getInputs() {
			return Collections.emptyList();
		}
	}

	/**
	 *
	 */
	public static class SqlTopnNode extends SingleSqlTreeNode {

		SqlTopnNode(SqlParserPos pos, SqlTreeNode input, String desc) {
			super(pos, SqlNodeType.TOPN, input, desc);
		}
	}

	/**
	 *
	 */
	public static class SqlUnionNode extends AbstractSqlTreeNode {

		private final List<SqlTreeNode> inputs;

		/**
		 * Creates a union node.
		 */
		SqlUnionNode(SqlParserPos pos, List<SqlTreeNode> inputs) {
			super(pos, SqlNodeType.UNION, "");
			this.inputs = inputs;
		}

		@Override
		public List<SqlTreeNode> getInputs() {
			return inputs;
		}
	}

	/**
	 *
	 */
	public static class SqlCepNode extends SingleSqlTreeNode {

		SqlCepNode(SqlParserPos pos, SqlTreeNode input, String desc) {
			super(pos, SqlNodeType.CEP, input, desc);
		}
	}
}
