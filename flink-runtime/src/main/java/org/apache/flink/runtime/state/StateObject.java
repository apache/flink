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

/**
 * Base of all types that represent checkpointed state. Specializations are for
 * example {@link StateHandle StateHandles} (directly resolve to state) and 
 * {@link KvStateSnapshot key/value state snapshots}.
 * 
 * <p>State objects define how to:
 * <ul>
 *     <li><b>Discard State</b>: The {@link #discardState()} method defines how state is permanently
 *         disposed/deleted. After that method call, state may not be recoverable any more.</li>
 
 *     <li><b>Close the current state access</b>: The {@link #close()} method defines how to
 *         stop the current access or recovery to the state. Called for example when an operation is
 *         canceled during recovery.</li>
 * </ul>
 */
public interface StateObject extends java.io.Closeable, java.io.Serializable {

	/**
	 * Discards the state referred to by this handle, to free up resources in
	 * the persistent storage. This method is called when the handle will not be
	 * used any more.
	 */
	void discardState() throws Exception;

	/**
	 * Returns the size of the state in bytes.
	 *
	 * <p>If the the size is not known, return {@code 0}.
	 *
	 * @return Size of the state in bytes.
	 * @throws Exception If the operation fails during size retrieval.
	 */
	long getStateSize() throws Exception;
}
