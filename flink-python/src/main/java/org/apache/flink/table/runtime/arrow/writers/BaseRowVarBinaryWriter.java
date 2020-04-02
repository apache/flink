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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.BaseRow;

import org.apache.arrow.vector.VarBinaryVector;

/**
 * {@link ArrowFieldWriter} for VarBinary.
 */
@Internal
public final class BaseRowVarBinaryWriter extends ArrowFieldWriter<BaseRow> {

	public BaseRowVarBinaryWriter(VarBinaryVector varBinaryVector) {
		super(varBinaryVector);
	}

	@Override
	public void doWrite(BaseRow row, int ordinal) {
		if (row.isNullAt(ordinal)) {
			((VarBinaryVector) getValueVector()).setNull(getCount());
		} else {
			((VarBinaryVector) getValueVector()).setSafe(getCount(), row.getBinary(ordinal));
		}
	}
}
