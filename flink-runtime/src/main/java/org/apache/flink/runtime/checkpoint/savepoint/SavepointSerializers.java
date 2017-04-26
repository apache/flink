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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.migration.runtime.checkpoint.savepoint.SavepointV0;
import org.apache.flink.migration.runtime.checkpoint.savepoint.SavepointV0Serializer;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper to access {@link SavepointSerializer} for a specific savepoint version.
 */
public class SavepointSerializers {


	private static final Map<Integer, SavepointSerializer<?>> SERIALIZERS = new HashMap<>(2);

	static {
		SERIALIZERS.put(SavepointV0.VERSION, SavepointV0Serializer.INSTANCE);
		SERIALIZERS.put(SavepointV1.VERSION, SavepointV1Serializer.INSTANCE);
		SERIALIZERS.put(SavepointV2.VERSION, SavepointV2Serializer.INSTANCE);
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the {@link SavepointSerializer} for the given savepoint.
	 *
	 * @param savepoint Savepoint to get serializer for
	 * @param <T>       Type of savepoint
	 * @return Savepoint serializer for the savepoint
	 * @throws IllegalArgumentException If unknown savepoint version
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Savepoint> SavepointSerializer<T> getSerializer(T savepoint) {
		Preconditions.checkNotNull(savepoint, "Savepoint");
		SavepointSerializer<T> serializer = (SavepointSerializer<T>) SERIALIZERS.get(savepoint.getVersion());
		if (serializer != null) {
			return serializer;
		} else {
			throw new IllegalArgumentException("Unknown savepoint version " + savepoint.getVersion() + ".");
		}
	}

	/**
	 * Returns the {@link SavepointSerializer} for the given savepoint version.
	 *
	 * @param version Savepoint version to get serializer for
	 * @return Savepoint for the given version
	 * @throws IllegalArgumentException If unknown savepoint version
	 */
	@SuppressWarnings("unchecked")
	public static SavepointSerializer<?> getSerializer(int version) {
		SavepointSerializer<?> serializer = SERIALIZERS.get(version);
		if (serializer != null) {
			return serializer;
		} else {
			throw new IllegalArgumentException("Cannot restore savepoint version " + version + ".");
		}
	}

}
