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
package org.apache.calcite.sql;

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

/**
 * Operator which aggregates sets of values into a result.
 */
public class SqlRankFunction extends SqlAggFunction {
	//~ Constructors -----------------------------------------------------------

	@Deprecated
	public SqlRankFunction(boolean requiresOrder, SqlKind kind) {
		this(kind, ReturnTypes.INTEGER, requiresOrder);
	}

	public SqlRankFunction(SqlKind kind, SqlReturnTypeInference returnTypes,
						   boolean requiresOrder) {
		super(kind.name(), null, kind, returnTypes, null,
			OperandTypes.NILADIC, SqlFunctionCategory.NUMERIC, requiresOrder,
			true, Optionality.FORBIDDEN);
	}

	//~ Methods ----------------------------------------------------------------

	@Override public boolean allowsFraming() {
		return true;
	}

}
