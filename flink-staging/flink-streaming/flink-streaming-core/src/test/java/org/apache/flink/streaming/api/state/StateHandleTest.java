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

package org.apache.flink.streaming.api.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StateHandleTest {

	@Test
	public void operatorStateHandleTest() throws Exception {

		MockHandle<Serializable> h1 = new MockHandle<Serializable>(1);

		OperatorStateHandle opHandle = new OperatorStateHandle(h1, true);
		assertEquals(1, opHandle.getState(this.getClass().getClassLoader()));

		OperatorStateHandle dsHandle = serializeDeserialize(opHandle);
		MockHandle<Serializable> h2 = (MockHandle<Serializable>) dsHandle.getHandle();
		assertFalse(h2.discarded);
		assertNotNull(h1.state);
		assertNull(h2.state);

		dsHandle.discardState();

		assertTrue(h2.discarded);
	}

	@Test
	public void wrapperStateHandleTest() throws Exception {
		final ClassLoader cl = this.getClass().getClassLoader();

		MockHandle<Serializable> h1 = new MockHandle<Serializable>(1);
		MockHandle<Serializable> h2 = new MockHandle<Serializable>(2);
		StateHandle<Serializable> h3 = new MockHandle<Serializable>(3);

		OperatorStateHandle opH1 = new OperatorStateHandle(h1, true);
		OperatorStateHandle opH2 = new OperatorStateHandle(h2, false);

		Map<String, OperatorStateHandle> opHandles = ImmutableMap.of("h1", opH1, "h2", opH2);

		Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>> fullState = Tuple2.of(h3,
				opHandles);

		List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>> chainedStates = ImmutableList
				.of(fullState);

		WrapperStateHandle wrapperHandle = new WrapperStateHandle(chainedStates);

		WrapperStateHandle dsWrapper = serializeDeserialize(wrapperHandle);

		@SuppressWarnings("unchecked")
		Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>> dsFullState = ((List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>>) dsWrapper
				.getState(cl)).get(0);

		Map<String, OperatorStateHandle> dsOpHandles = dsFullState.f1;

		assertNull(dsFullState.f0.getState(cl));
		assertFalse(((MockHandle<?>) dsFullState.f0).discarded);
		assertFalse(((MockHandle<?>) dsOpHandles.get("h1").getHandle()).discarded);
		assertNull(dsOpHandles.get("h1").getState(cl));
		assertFalse(((MockHandle<?>) dsOpHandles.get("h2").getHandle()).discarded);
		assertNull(dsOpHandles.get("h2").getState(cl));

		dsWrapper.discardState();

		assertTrue(((MockHandle<?>) dsFullState.f0).discarded);
		assertTrue(((MockHandle<?>) dsOpHandles.get("h1").getHandle()).discarded);
		assertTrue(((MockHandle<?>) dsOpHandles.get("h2").getHandle()).discarded);

	}

	@SuppressWarnings("unchecked")
	private <X extends StateHandle<?>> X serializeDeserialize(X handle) throws IOException,
			ClassNotFoundException {
		byte[] serialized = InstantiationUtil.serializeObject(handle);
		return (X) InstantiationUtil.deserializeObject(serialized, Thread.currentThread()
				.getContextClassLoader());
	}

	@SuppressWarnings("serial")
	private static class MockHandle<T> implements StateHandle<T> {

		boolean discarded = false;
		transient T state;

		public MockHandle(T state) {
			this.state = state;
		}

		@Override
		public void discardState() {
			state = null;
			discarded = true;
		}

		@Override
		public T getState(ClassLoader userCodeClassLoader) {
			return state;
		}
	}

}
