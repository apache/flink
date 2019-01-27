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

/**
 * Enumerates the possible types of {@link SqlTreeNode}.
 */
public enum SqlNodeType {

	/**
	 * Source table.
	 */
	SOURCE,

	/**
	 * Sink table.
	 */
	SINK,

	/**
	 * Dimension table.
	 */
	DIM,

	/**
	 * SELECT statement or sub-query.
	 */
	SELECT,

	/**
	 * Where clause.
	 */
	FILTER,

	/**
	 * Group Aggregate.
	 */
	GROUP,

	/**
	 * JOIN operator or compound FROM clause.
	 *
	 * <p>A FROM clause with more than one table is represented as if it were a
	 * join. For example, "FROM x, y, z" is represented as
	 * "JOIN(x, JOIN(x, y))".
	 */
	JOIN,

	/**
	 * Union.
	 */
	UNION,

	/**
	 * TopN.
	 */
	TOPN,

	/**
	 * CEP clause.
	 */
	CEP,

	/**
	 * udtf.
	 */
	UDTF,

	/**
	 * other nodes.
	 */
	OTHER
}
