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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

// This class is copied from Calcite's org.apache.calcite.sql.validate.ParameterScope,
// can be removed after https://issues.apache.org/jira/browse/CALCITE-3476 is fixed.
//
// Modification:
// - L66~L69: override resolveColumn method

/**
 * A scope which contains nothing besides a few parameters. Like
 * {@link EmptyScope} (which is its base class), it has no parent scope.
 *
 * @see ParameterNamespace
 */
public class ParameterScope extends EmptyScope {
	//~ Instance fields --------------------------------------------------------

	/**
	 * Map from the simple names of the parameters to types of the parameters
	 * ({@link RelDataType}).
	 */
	private final Map<String, RelDataType> nameToTypeMap;

	//~ Constructors -----------------------------------------------------------

	public ParameterScope(
		SqlValidatorImpl validator,
		Map<String, RelDataType> nameToTypeMap) {
		super(validator);
		this.nameToTypeMap = nameToTypeMap;
	}

	//~ Methods ----------------------------------------------------------------

	public SqlQualified fullyQualify(SqlIdentifier identifier) {
		return SqlQualified.create(this, 1, null, identifier);
	}

	public SqlValidatorScope getOperandScope(SqlCall call) {
		return this;
	}

	@Override
	public RelDataType resolveColumn(String name, SqlNode ctx) {
		return nameToTypeMap.get(name);
	}
}

// End ParameterScope.java
