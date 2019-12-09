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

package org.apache.flink.table.planner.functions.tablefunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Replicate the row N times. N is specified as the first argument to the function.
 * This is an internal function solely used by optimizer to rewrite EXCEPT ALL AND
 * INTERSECT ALL queries.
 */
public class ReplicateRows extends TableFunction<Row> {

	private static final long serialVersionUID = 1L;

	private final TypeInformation[] fieldTypes;
	private transient Row reuseRow;

	public ReplicateRows(TypeInformation[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	public void eval(Object... inputs) {
		checkArgument(inputs.length == fieldTypes.length + 1);
		long numRows = (long) inputs[0];
		if (reuseRow == null) {
			reuseRow = new Row(fieldTypes.length);
		}
		for (int i = 0; i < fieldTypes.length; i++) {
			reuseRow.setField(i, inputs[i + 1]);
		}
		for (int i = 0; i < numRows; i++) {
			collect(reuseRow);
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		TypeInformation[] paraTypes = new TypeInformation[1 + fieldTypes.length];
		paraTypes[0] = Types.LONG;
		System.arraycopy(fieldTypes, 0, paraTypes, 1, fieldTypes.length);
		return paraTypes;
	}
}
