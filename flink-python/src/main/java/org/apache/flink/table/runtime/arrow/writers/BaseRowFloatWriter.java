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

import org.apache.arrow.vector.Float4Vector;

/**
 * {@link ArrowFieldWriter} for Float.
 */
@Internal
public final class BaseRowFloatWriter extends ArrowFieldWriter<BaseRow> {

	public BaseRowFloatWriter(Float4Vector floatVector) {
		super(floatVector);
	}

	@Override
	public void doWrite(BaseRow row, int ordinal) {
		if (row.isNullAt(ordinal)) {
			((Float4Vector) getValueVector()).setNull(getCount());
		} else {
			((Float4Vector) getValueVector()).setSafe(getCount(), row.getFloat(ordinal));
		}
	}
}
