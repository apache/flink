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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;

/**
 * A test for the {@link BinaryRowSerializer}.
 */
public class BinaryRowSerializerTest extends SerializerTestBase<BinaryRow> {

	@Override
	protected BinaryRowSerializer createSerializer() {
		return new BinaryRowSerializer(2);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BinaryRow> getTypeClass() {
		return BinaryRow.class;
	}

	@Override
	protected BinaryRow[] getTestData() {
		return new BinaryRow[] {
				createRow("11", 1),
				createRow("12", 2),
				createRow("132", 3),
				createRow("13", 4)
		};
	}

	private static BinaryRow createRow(String f0, int f1) {
		BinaryRow row = new BinaryRow(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, BinaryString.fromString(f0));
		writer.writeInt(1, f1);
		writer.complete();
		return row;
	}
}
