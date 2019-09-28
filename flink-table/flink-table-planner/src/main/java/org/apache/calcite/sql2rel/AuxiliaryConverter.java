/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql2rel;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL CALCITE-1761 IS FIXED.
 */

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Converts an expression for a group window function (e.g. TUMBLE)
 * into an expression for an auxiliary group function (e.g. TUMBLE_START).
 *
 * @see SqlStdOperatorTable#TUMBLE
 */
public interface AuxiliaryConverter {
	/** Converts an expression.
	 *
	 * @param rexBuilder Rex  builder
	 * @param groupCall Call to the group function, e.g. "TUMBLE($2, 36000)"
	 * @param e Expression holding result of the group function, e.g. "$0"
	 *
	 * @return Expression for auxiliary function, e.g. "$0 + 36000" converts
	 * the result of TUMBLE to the result of TUMBLE_END
	 */
	RexNode convert(RexBuilder rexBuilder, RexNode groupCall, RexNode e);

	/** Simple implementation of {@link AuxiliaryConverter}. */
	class Impl implements AuxiliaryConverter {
		private final SqlFunction f;

		public Impl(SqlFunction f) {
			this.f = f;
		}

		public RexNode convert(RexBuilder rexBuilder, RexNode groupCall,
			RexNode e) {
			return rexBuilder.makeCall(this.f, e);
			// FLINK QUICK FIX
			// we do not use this logic right now
//      switch (f.getKind()) {
//      case TUMBLE_START:
//      case HOP_START:
//      case SESSION_START:
//      case SESSION_END: // TODO: ?
//        return e;
//      case TUMBLE_END:
//        return rexBuilder.makeCall(
//            SqlStdOperatorTable.PLUS, e,
//            ((RexCall) groupCall).operands.get(1));
//      case HOP_END:
//        return rexBuilder.makeCall(
//            SqlStdOperatorTable.PLUS, e,
//            ((RexCall) groupCall).operands.get(2));
//      default:
//        throw new AssertionError("unknown: " + f);
//      }
		}
	}
}

// End AuxiliaryConverter.java
