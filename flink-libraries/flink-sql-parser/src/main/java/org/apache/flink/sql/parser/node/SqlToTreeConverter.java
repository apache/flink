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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a SQL parse tree. (consisting of
 * {@link org.apache.calcite.sql.SqlNode} objects) into a relational node tree
 * (consisting of {@link SqlTreeNode} objects)
 */
public class SqlToTreeConverter {

	private final Map<String, SqlTreeNode> context = new HashMap<>();
	private final List<SqlTreeNode> nodes = new ArrayList<>();
	private final SqlValidator validator;

	public SqlToTreeConverter(SqlValidator validator) {
		this.validator = validator;
	}

	/**
	 * Converts a query's parse tree into a relation tree.
	 *
	 * @param query sql query parser tree
	 */
	public void convertSql(SqlNode query) {
		SqlTreeNode node = convertQueryRecursive(query);
		if (node != null) {
			nodes.add(node);
		}
	}

	public String getJSON() {
		SqlTreeJSONGenerator generator = new SqlTreeJSONGenerator();
		return generator.getJSON(nodes);
	}

	/**
	 * Recursively converts a query to a tree node.
	 *
	 * @param query Query
	 * @return Relational expression
	 */
	private SqlTreeNode convertQueryRecursive(SqlNode query) {
		final SqlKind kind = query.getKind();
		switch (kind) {
			case SELECT:
				return convertSelect((SqlSelect) query);
			case INSERT:
				return convertInsert((SqlInsert) query);
			case CREATE_VIEW:
				return convertView((SqlCreateView) query);
			case CREATE_TABLE:
				return registerTable((SqlCreateTable) query);
			case UNION:
			case INTERSECT:
			case EXCEPT:
				return convertSetOp((SqlCall) query);
			case OTHER_DDL:
				// ignore create function ...
				return null;
			default:
				throw new AssertionError("not a query: " + query);
		}
	}

	private SqlTreeNode convertView(SqlCreateView view) {
		String viewName = view.getName();
		SqlTreeNode input = convertQueryRecursive(view.getQuery());
		context.put(viewName, input);
		return input;
	}

	private SqlTreeNode convertInsert(SqlInsert insert) {
		SqlTreeNode input = convertQueryRecursive(insert.getSource());
		String tableName = insert.getTargetTable().toString();
		return SqlTreeNodes.sink(insert.getParserPosition(), input, tableName);

	}

	private SqlTreeNode convertSelect(SqlSelect query) {
		SqlTreeNode input = convertFrom(query.getFrom());
		SqlTreeNode filter = convertWhere(input, query.getWhere());
		if (filter != null) {
			input = filter;
		}

		if (validator.isAggregate(query)) {
			SqlNodeList groupList = query.getGroup();
			input = SqlTreeNodes.group(groupList.getParserPosition(), input, groupList);
		} else {
			SqlNodeList selectList = query.getSelectList();
			input = SqlTreeNodes.select(query.getParserPosition(), input, selectList);
		}
		return convertOrder(query, input);
	}

	private SqlTreeNode convertSetOp(SqlCall call) {
		final SqlTreeNode left = convertQueryRecursive(call.operand(0));
		final SqlTreeNode right = convertQueryRecursive(call.operand(1));
		switch (call.getKind()) {
			case UNION:
				return SqlTreeNodes.union(call.getParserPosition(), ImmutableList.of(left, right));

			case INTERSECT:
				throw Util.unexpected(call.getKind());

			case EXCEPT:
				throw Util.unexpected(call.getKind());

			default:
				throw Util.unexpected(call.getKind());
		}
	}

	private SqlTreeNode convertOrder(SqlSelect select, SqlTreeNode input) {
		SqlNodeList orderBy = select.getOrderList();
		if (orderBy == null || orderBy.getList().isEmpty()) {
			return input;
		}

		return SqlTreeNodes.topn(orderBy.getParserPosition(), input, "");
	}

	private SqlTreeNode convertWhere(SqlTreeNode input, SqlNode where) {
		if (where == null) {
			return null;
		}
		return SqlTreeNodes.filter(where.getParserPosition(), input, where);
	}

	private SqlTreeNode convertFrom(SqlNode from) {
		SqlCall call;
		switch (from.getKind()) {
			case IDENTIFIER:
				String tableName = from.toString();
				return context.get(tableName);
			case AS:
				return convertFrom(((SqlCall) from).operand(0));
			case MATCH_RECOGNIZE:
				return convertMatchRecognize((SqlCall) from);
			case SNAPSHOT:
				return convertTemporal((SqlSnapshot) from);
			case JOIN:
				return convertJoin((SqlJoin) from);
			case SELECT:
			case INTERSECT:
			case EXCEPT:
			case UNION:
				return convertQueryRecursive(from);
			case LATERAL:
				call = (SqlCall) from;
				return convertFrom((call.getOperandList().get(0)));
			case COLLECTION_TABLE:
				call = (SqlCall) from;
				return SqlTreeNodes.udtf(from.getParserPosition(), call.getOperandList().get(0));
			case VALUES:
				throw new UnsupportedOperationException();
			default:
				throw new AssertionError("Not supported operator " + from);
		}
	}

	private SqlTreeNode convertJoin(SqlJoin join) {
		SqlNode left = join.getLeft();
		SqlNode right = join.getRight();
		SqlTreeNode leftNode = convertFrom(left);
		SqlTreeNode rightNode = convertFrom(right);
		if (rightNode instanceof SqlTreeNodes.SqlTableFunctionNode) {
			// udtf
			return SqlTreeNodes.correlate(join.getParserPosition(), leftNode, rightNode.explain());
		} else {
			JoinType joinType = join.getJoinType();
			return SqlTreeNodes.join(join.getParserPosition(), leftNode, rightNode, joinType);
		}
	}

	private SqlTreeNode convertTemporal(SqlSnapshot call) {
		String tableName = call.getTableRef().toString();
		return context.get(tableName);
	}

	private SqlTreeNode convertMatchRecognize(SqlCall call) {
		SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
		SqlNode tableRef = matchRecognize.getTableRef();
		SqlTreeNode input = convertFrom(tableRef);
		SqlNode pattern = matchRecognize.getPattern();
		return SqlTreeNodes.cep(call.getParserPosition(), input, pattern);
	}

	/**
	 * Register source and dim tables.
	 */
	public SqlTreeNode registerTable(SqlCreateTable node) {
		SqlParserPos pos = node.getParserPosition();
		String tableName = node.getTableName().toString();
		SqlTreeNode table;
		switch (node.getTableType()) {
			case "SOURCE":
				table = SqlTreeNodes.source(pos, tableName);
				context.put(tableName, table);
				return table;
			case "SINK":
				// ignore
				return null;
			default:
				throw new IllegalArgumentException(
					"Illegal table type of SqlCreateTable: " + node.getTableType());
		}
	}
}
