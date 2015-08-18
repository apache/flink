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

package org.apache.flink.runtime.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;

public class ByteStreamStateHandleTest {

	@Test
	public void testHandle() throws Exception {
		final ClassLoader cl = this.getClass().getClassLoader();
		MockHandle handle;

		try {
			handle = new MockHandle(null);
			fail();
		} catch (RuntimeException e) {
			// expected behaviour
		}

		handle = new MockHandle(1);

		assertEquals(1, handle.getState(cl));
		assertTrue(handle.stateFetched());
		assertFalse(handle.isWritten());
		assertFalse(handle.discarded);

		MockHandle handleDs = serializeDeserialize(handle);

		assertEquals(1, handle.getState(cl));
		assertTrue(handle.stateFetched());
		assertTrue(handle.isWritten());
		assertTrue(handle.generatedOutput);
		assertFalse(handle.discarded);

		assertFalse(handleDs.stateFetched());
		assertTrue(handleDs.isWritten());
		assertFalse(handleDs.generatedOutput);
		assertFalse(handle.discarded);

		try {
			handleDs.getState(cl);
			fail();
		} catch (UnsupportedOperationException e) {
			// good
		}

		MockHandle handleDs2 = serializeDeserialize(handleDs);

		assertFalse(handleDs2.stateFetched());
		assertTrue(handleDs2.isWritten());
		assertFalse(handleDs.generatedOutput);
		assertFalse(handleDs2.generatedOutput);
		assertFalse(handleDs2.discarded);

		handleDs2.discardState();
		assertTrue(handleDs2.discarded);

	}

	@SuppressWarnings("unchecked")
	private <X extends StateHandle<?>> X serializeDeserialize(X handle) throws IOException,
			ClassNotFoundException {
		byte[] serialized = InstantiationUtil.serializeObject(handle);
		return (X) InstantiationUtil.deserializeObject(serialized, Thread.currentThread()
				.getContextClassLoader());
	}

	private static class MockHandle extends ByteStreamStateHandle {

		private static final long serialVersionUID = 1L;

		public MockHandle(Serializable state) {
			super(state);
		}

		boolean discarded = false;
		transient boolean generatedOutput = false;

		@Override
		public void discardState() throws Exception {
			discarded = true;
		}

		@Override
		protected OutputStream getOutputStream() throws Exception {
			generatedOutput = true;
			return new ByteArrayOutputStream();
		}

		@Override
		protected InputStream getInputStream() throws Exception {
			throw new UnsupportedOperationException();
		}

	}

}
