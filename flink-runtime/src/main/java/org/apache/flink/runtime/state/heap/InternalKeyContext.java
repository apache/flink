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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * This interface is the current context of a keyed state. It provides information about the currently selected key in
 * the context, the corresponding key-group, and other key and key-grouping related information.
 * <p>
 * The typical use case for this interface is providing a view on the current-key selection aspects of
 * {@link org.apache.flink.runtime.state.KeyedStateBackend}.
 */
@Internal
public interface InternalKeyContext<K> {

	/**
	 * Used by states to access the current key.
	 */
	K getCurrentKey();

	/**
	 * Returns the key-group to which the current key belongs.
	 */
	int getCurrentKeyGroupIndex();

	/**
	 * Returns the number of key-groups aka max parallelism.
	 */
	int getNumberOfKeyGroups();

	/**
	 * Returns the key groups for this backend.
	 */
	KeyGroupRange getKeyGroupRange();

	/**
	 * {@link TypeSerializer} for the state backend key type.
	 */
	TypeSerializer<K> getKeySerializer();

}
