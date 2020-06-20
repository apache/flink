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

package org.apache.flink.table.client.cli.utils;

import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utilities for terminal handling.
 */
public class TerminalUtils {

	private TerminalUtils() {
		// do not instantiate
	}

	public static Terminal createDummyTerminal() {
		return createDummyTerminal(new MockOutputStream());
	}

	public static Terminal createDummyTerminal(OutputStream out) {
		try {
			return new DumbTerminal(new MockInputStream(), out);
		} catch (IOException e) {
			throw new RuntimeException("Unable to create dummy terminal.");
		}
	}

	private static class MockInputStream extends InputStream {

		@Override
		public int read() {
			return 0;
		}
	}

	/**
	 * A mock {@link OutputStream} for testing.
	 */
	public static class MockOutputStream extends OutputStream {

		@Override
		public void write(int b) {
			// do nothing
		}
	}
}
