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

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AbstractCloseableHandleTest {

	@Test
	public void testRegisterThenClose() throws Exception {
		Closeable closeable = mock(Closeable.class);

		AbstractCloseableHandle handle = new CloseableHandle();
		assertFalse(handle.isClosed());

		// no immediate closing
		handle.registerCloseable(closeable);
		verify(closeable, times(0)).close();
		assertFalse(handle.isClosed());

		// close forwarded once
		handle.close();
		verify(closeable, times(1)).close();
		assertTrue(handle.isClosed());

		// no repeated closing
		handle.close();
		verify(closeable, times(1)).close();
		assertTrue(handle.isClosed());
	}

	@Test
	public void testCloseThenRegister() throws Exception {
		Closeable closeable = mock(Closeable.class);

		AbstractCloseableHandle handle = new CloseableHandle();
		assertFalse(handle.isClosed());

		// close the handle before setting the closeable
		handle.close();
		assertTrue(handle.isClosed());

		// immediate closing
		try {
			handle.registerCloseable(closeable);
			fail("this should throw an excepion");
		} catch (IOException e) {
			// expected
			assertTrue(e.getMessage().contains("closed"));
		}

		// should still have called "close" on the Closeable
		verify(closeable, times(1)).close();
		assertTrue(handle.isClosed());

		// no repeated closing
		handle.close();
		verify(closeable, times(1)).close();
		assertTrue(handle.isClosed());
	}

	// ------------------------------------------------------------------------

	private static final class CloseableHandle extends AbstractCloseableHandle {
		private static final long serialVersionUID = 1L;
	}
}
