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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractGenericCloseableRegistry;
import org.apache.flink.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * This class allows to register instances of {@link Closeable}, which are all closed if this registry is closed.
 * Typically, only the owning object should work with the full interface of this class, whereas other client code
 * should only use the {@link CloseableRegistryClientView} to interact with this class.
 * <p>
 * Registering to an already closed registry will throw an exception and close the provided {@link Closeable}
 * <p>
 * All methods in this class are thread-safe.
 */
@Internal
public class OwnedCloseableRegistry
		extends AbstractGenericCloseableRegistry<Closeable, Object>
		implements CloseableRegistryClientView {

	private static final Object DUMMY = new Object();

	@Override
	protected void doUnRegistering(Closeable closeable, Map<Closeable, Object> closeableMap) {
		closeableMap.remove(closeable);
	}

	@Override
	protected void doRegistering(Closeable closeable, Map<Closeable, Object> closeableMap) throws IOException {

		if (closed) {
			IOUtils.closeQuietly(closeable);
			throw new IOException("Cannot register Closeable: Registry is already closed. Closing argument.");
		}

		closeableMap.put(closeable, DUMMY);
	}

	@Override
	protected void doClosing(Map<Closeable, Object> closeableMap) {
		IOUtils.closeAllQuietly(closeableMap.keySet());
		closeableMap.clear();
	}
}
