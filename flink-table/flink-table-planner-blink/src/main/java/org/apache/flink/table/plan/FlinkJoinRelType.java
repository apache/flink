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

package org.apache.flink.table.plan;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;

/**
 * Enumeration of join types.
 */
public enum FlinkJoinRelType {
	INNER, LEFT, RIGHT, FULL, SEMI, ANTI;

	public boolean isOuter() {
		switch (this) {
			case LEFT:
			case RIGHT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	public boolean isLeftOuter() {
		switch (this) {
			case LEFT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	public boolean isRightOuter() {
		switch (this) {
			case RIGHT:
			case FULL:
				return true;
			default:
				return false;
		}
	}

	/** Convert JoinRelType to FlinkJoinRelType. */
	public static FlinkJoinRelType toFlinkJoinRelType(JoinRelType joinType) {
		switch (joinType) {
			case INNER:
				return FlinkJoinRelType.INNER;
			case LEFT:
				return FlinkJoinRelType.LEFT;
			case RIGHT:
				return FlinkJoinRelType.RIGHT;
			case FULL:
				return FlinkJoinRelType.FULL;
			default:
				throw new IllegalArgumentException("invalid: " + joinType);
		}
	}

	public static FlinkJoinRelType getFlinkJoinRelType(Join join) {
		if (join instanceof SemiJoin) {
			// TODO supports ANTI
			return SEMI;
		} else {
			return toFlinkJoinRelType(join.getJoinType());
		}
	}

	/** Convert FlinkJoinRelType to JoinRelType. */
	public static JoinRelType toJoinRelType(FlinkJoinRelType joinType) {
		switch (joinType) {
			case INNER:
				return JoinRelType.INNER;
			case LEFT:
				return JoinRelType.LEFT;
			case RIGHT:
				return JoinRelType.RIGHT;
			case FULL:
				return JoinRelType.FULL;
			default:
				throw new IllegalArgumentException("invalid: " + joinType);
		}
	}
}

