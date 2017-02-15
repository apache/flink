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

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for client code un/registering @{@link Closeable} objects to a registry. This registry is typically owned
 * and provided from another object.
 */
@Internal
public interface CloseableRegistryClientView {

	/**
	 * Register a closeable object.
	 *
	 * @param closeable object to register
	 * @throws IOException if the registry is already closed. The argument closeable will also be closed in this case.
	 */
	void register(Closeable closeable) throws IOException;

	/**
	 * Unregister a closeable object.
	 *
	 * @param closeable object to unregister
	 */
	void unregister(Closeable closeable);
}
