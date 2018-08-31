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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link ImmutableListState}.
 */
public class ImmutableListStateTest {

	private final ListStateDescriptor<Long> listStateDesc =
			new ListStateDescriptor<>("test", BasicTypeInfo.LONG_TYPE_INFO);

	private ListState<Long> listState;

	@Before
	public void setUp() throws Exception {
		if (!listStateDesc.isSerializerInitialized()) {
			listStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		List<Long> init = new ArrayList<>();
		init.add(42L);

		byte[] serInit = serializeInitValue(init);
		listState = ImmutableListState.createState(listStateDesc, serInit);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUpdate() throws Exception {
		List<Long> list = getStateContents();
		assertEquals(1L, list.size());

		long element = list.get(0);
		assertEquals(42L, element);

		listState.add(54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() throws Exception {
		List<Long> list = getStateContents();
		assertEquals(1L, list.size());

		long element = list.get(0);
		assertEquals(42L, element);

		listState.clear();
	}

	/**
	 * Copied from HeapListState.getSerializedValue(Object, Object).
	 */
	private byte[] serializeInitValue(List<Long> toSerialize) throws IOException {
		TypeSerializer<Long> serializer = listStateDesc.getElementSerializer();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		// write the same as RocksDB writes lists, with one ',' separator
		for (int i = 0; i < toSerialize.size(); i++) {
			serializer.serialize(toSerialize.get(i), view);
			if (i < toSerialize.size() - 1) {
				view.writeByte(',');
			}
		}
		view.flush();

		return baos.toByteArray();
	}

	private List<Long> getStateContents() throws Exception {
		List<Long> list = new ArrayList<>();
		for (Long elem: listState.get()) {
			list.add(elem);
		}
		return list;
	}
}
