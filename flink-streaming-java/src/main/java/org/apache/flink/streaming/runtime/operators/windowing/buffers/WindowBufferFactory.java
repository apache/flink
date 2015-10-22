/**
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
package org.apache.flink.streaming.runtime.operators.windowing.buffers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * A factory for {@link WindowBuffer WindowBuffers}.
 *
 * @param <T> The type of elements that the created {@code WindowBuffer} can store.
 * @param <B> The type of the created {@code WindowBuffer}
 */
public interface WindowBufferFactory<T, B extends WindowBuffer<T>> extends Serializable {

	/**
	 * Sets the {@link RuntimeContext} that is used to initialize eventual user functions
	 * inside the created buffers.
	 */
	void setRuntimeContext(RuntimeContext ctx);

	/**
	 * Calls {@code open()} on eventual user functions inside the buffer.
	 */
	void open(Configuration config) throws Exception;

	/**
	 * Calls {@code close()} on eventual user functions inside the buffer.
	 */

	void close() throws Exception;

	/**
	 * Creates a new {@code WindowBuffer}.
	 */
	B create();
}
