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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.flink.types.Row;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * Tests for {@link RowCoder}.
 */
public class RowCoderTest extends CoderTestBase<Row> {

	@Override
	protected Coder<Row> createCoder() {
		Coder<?>[] fieldCoders = {
			VarLongCoder.of(),
			VarLongCoder.of()};
		return new RowCoder(fieldCoders);
	}

	@Override
	protected Row[] getTestData() {
		Row row1 = new Row(2);
		row1.setField(0, 1L);
		row1.setField(1, -1L);

		Row row2 = new Row(2);
		row2.setField(0, 2L);
		row2.setField(1, null);

		return new Row[]{row1, row2};
	}
}
