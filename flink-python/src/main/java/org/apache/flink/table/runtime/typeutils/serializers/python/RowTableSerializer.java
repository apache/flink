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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * The implementation of TableSerializer in legacy planner.
 */
@Internal
public final class RowTableSerializer extends TableSerializer<Row> {

	private final RowSerializer rowSerializer;

	public RowTableSerializer(TypeSerializer<?>[] fieldSerializers) {
		super(fieldSerializers);
		this.rowSerializer = new RowSerializer(fieldSerializers);
	}

	@Override
	public Row createResult(int len) {
		return new Row(len);
	}

	@Override
	public void setField(Row result, int index, Object value) {
		result.setField(index, value);
	}

	@Override
	public void serialize(Row record, DataOutputView target) throws IOException {
		rowSerializer.serialize(record, target);
	}

	public RowSerializer getRowSerializer() {
		return rowSerializer;
	}
}
