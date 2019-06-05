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

package org.apache.flink.table.sqlexec;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be
 * executed directly.
 *
 * <p>For every kind of {@link SqlNode}, there needs a method named
 * #execute(type), the 'type' argument should be the subclass
 * type for the supported {@link SqlNode}.
 */
public class SqlExecutableStatements implements ReflectiveVisitor {
	private TableEnvironment tableEnv;

	private final ReflectUtil.MethodDispatcher<Void> dispatcher =
		ReflectUtil.createMethodDispatcher(Void.class,
			this,
			"execute",
			SqlNode.class);

	//~ Constructors -----------------------------------------------------------

	private SqlExecutableStatements(TableEnvironment tableEnvironment) {
		this.tableEnv = tableEnvironment;
	}

	/**
	 * This is the main entrance of executing all kinds of DDL/DML {@code SqlNode}s, different
	 * SqlNode will have it's implementation in the #execute(type) method whose 'type' argument
	 * is subclass of {@code SqlNode}.
	 *
	 * <p>Caution that the {@link #execute(SqlNode)} should never expect to be invoked.
	 *
	 * @param tableEnvironment TableEnvironment to interact with
	 * @param sqlNode          SqlNode to execute on
	 */
	public static void executeSqlNode(TableEnvironment tableEnvironment, SqlNode sqlNode) {
		SqlExecutableStatements statement = new SqlExecutableStatements(tableEnvironment);
		statement.dispatcher.invoke(sqlNode);
	}

	/**
	 * Execute the {@link SqlCreateTable} node.
	 */
	public void execute(SqlCreateTable sqlCreateTable) {
		// need to implement.
	}

	/** Fallback method to throw exception. */
	public void execute(SqlNode node) {
		throw new TableException("Should not invoke to node type "
			+ node.getClass().getSimpleName());
	}
}
