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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * This class represents a snapshot of the {@link KvState}, taken for a checkpoint. Where exactly
 * the snapshot stores the snapshot data (in this object, in an external data store, etc) depends
 * on the actual implementation. This snapshot defines merely how to restore the state and
 * how to discard the state.
 *
 * <p>One possible implementation is that this snapshot simply contains a copy of the key/value map.
 * 
 * <p>Another possible implementation for this snapshot is that the key/value map is serialized into
 * a file and this snapshot object contains a pointer to that file.
 *
 * @param <K> The type of the key
 * @param <Backend> The type of the backend that can restore the state from this snapshot.
 */
public interface KvStateSnapshot<K, S extends State, SI extends StateIdentifier<S>, Backend extends AbstractStateBackend> extends java.io.Serializable {

	/**
	 * Loads the key/value state back from this snapshot.
	 * 
	 * 
	 * @param stateBackend The state backend that created this snapshot and can restore the key/value state
	 *                     from this snapshot.
	 * @param keySerializer The serializer for the keys.
	 * @param stateIdentifier The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param classLoader The class loader for user-defined types.
	 * 
	 * @return An instance of the key/value state loaded from this snapshot.
	 * 
	 * @throws Exception Exceptions can occur during the state loading and are forwarded. 
	 */
	KvState<K, S, SI, Backend> restoreState(
			Backend stateBackend,
			TypeSerializer<K> keySerializer,
			SI stateIdentifier,
			ClassLoader classLoader) throws Exception;


	/**
	 * Discards the state snapshot, removing any resources occupied by it.
	 * 
	 * @throws Exception Exceptions occurring during the state disposal should be forwarded.
	 */
	void discardState() throws Exception;
}
