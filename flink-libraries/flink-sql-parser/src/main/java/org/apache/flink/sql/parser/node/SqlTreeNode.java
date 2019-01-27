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

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * A SqlTreeNode is a node of SQL relation tree.
 */
public interface SqlTreeNode {

	SqlParserPos getParserPosition();

	/**
	 * Returns the type of this node.
	 *
	 * @return a {@link SqlNodeType} value, never null
	 */
	SqlNodeType getNodeType();

	String explain();

	/**
	 * Returns the <code>i</code><sup>th</sup> input node.
	 *
	 * @param i Ordinal of input
	 * @return <code>i</code><sup>th</sup> input
	 */
	SqlTreeNode getInput(int i);

	/**
	 * Returns an array of this node's inputs. If there are no
	 * inputs, returns an empty list, not {@code null}.
	 *
	 * @return Array of this node's inputs
	 */
	List<SqlTreeNode> getInputs();

}
