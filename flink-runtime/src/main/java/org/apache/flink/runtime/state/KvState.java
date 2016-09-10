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
 * Key/Value state implementation for user-defined state. The state is backed by a state
 * backend, which typically follows one of the following patterns: Either the state is stored
 * in the key/value state object directly (meaning in the executing JVM) and snapshotted by the
 * state backend into some store (during checkpoints), or the key/value state is in fact backed
 * by an external key/value store as the state backend, and checkpoints merely record the
 * metadata of what is considered part of the checkpoint.
 * 
 * @param <N> The type of the namespace.
 */
public interface KvState<N> {

	/**
	 * Sets the current namespace, which will be used when using the state access methods.
	 *
	 * @param namespace The namespace.
	 */
	void setCurrentNamespace(N namespace);

	/**
	 * Returns the serialized value for the given key and namespace.
	 *
	 * <p>If no value is associated with key and namespace, <code>null</code>
	 * is returned.
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @return Serialized value or <code>null</code> if no value is associated
	 * with the key and namespace.
	 * @throws Exception Exceptions during serialization are forwarded
	 */
	byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception;
}
