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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.calcite.rel.core.JoinRelType;

/**
 * Utility for {@link FlinkJoinType}.
 */
public class JoinTypeUtil {

	/**
	 * Converts {@link JoinRelType} to {@link FlinkJoinType}.
	 */
	public static FlinkJoinType getFlinkJoinType(JoinRelType joinRelType) {
		switch (joinRelType) {
			case INNER:
				return FlinkJoinType.INNER;
			case LEFT:
				return FlinkJoinType.LEFT;
			case RIGHT:
				return FlinkJoinType.RIGHT;
			case FULL:
				return FlinkJoinType.FULL;
			case SEMI:
				return FlinkJoinType.SEMI;
			case ANTI:
				return FlinkJoinType.ANTI;
			default:
				throw new IllegalArgumentException("invalid: " + joinRelType);
		}
	}

}
