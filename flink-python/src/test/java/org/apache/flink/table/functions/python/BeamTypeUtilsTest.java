/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions.python;

import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.runtime.typeutils.BeamTypeUtils;
import org.apache.flink.table.runtime.typeutils.coders.BaseRowCoder;
import org.apache.flink.table.runtime.typeutils.coders.RowCoder;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BeamTypeUtils}.
 */
public class BeamTypeUtilsTest {

	@Test
	public void testLogicalTypeToCoder() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		Coder coder = BeamTypeUtils.toCoder(rowType);
		assertTrue(coder instanceof RowCoder);

		Coder<?>[] fieldCoders = ((RowCoder) coder).getFieldCoders();
		assertEquals(1, fieldCoders.length);
		assertTrue(fieldCoders[0] instanceof VarLongCoder);
	}

	@Test
	public void testLogicalTypeToBlinkCoder() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		Coder coder = BeamTypeUtils.toBlinkCoder(rowType);
		assertTrue(coder instanceof BaseRowCoder);

		Coder<?>[] fieldCoders = ((BaseRowCoder) coder).getFieldCoders();
		assertEquals(1, fieldCoders.length);
		assertTrue(fieldCoders[0] instanceof VarLongCoder);
	}

	@Test
	public void testLogicalTypeToProto() {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("f1", new BigIntType()));
		RowType rowType = new RowType(rowFields);
		FlinkFnApi.Schema.FieldType protoType = BeamTypeUtils.toProtoType(rowType);
		FlinkFnApi.Schema schema = protoType.getRowSchema();
		assertEquals(1, schema.getFieldsCount());
		assertEquals("f1", schema.getFields(0).getName());
		assertEquals(FlinkFnApi.Schema.TypeName.BIGINT, schema.getFields(0).getType().getTypeName());
	}
}
