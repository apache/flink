/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Utilities related to serializer compatibility.
 */
@Internal
public class CompatibilityUtil {

	/**
	 * Resolves the final compatibility result of two serializers by taking into account compound information,
	 * including the preceding serializer, the preceding serializer's configuration snapshot, and the new serializer.
	 * This method has the side effect that the provided new serializer may have been reconfigured in order to
	 * remain compatible.
	 *
	 * The final result is determined as follows:
	 *   1. If there is no configuration snapshot of the preceding serializer,
	 *      assumes the new serializer to be compatible.
	 *   2. Confront the configuration snapshot with the new serializer.
	 *   3. If the result is compatible, just return that as the result.
	 *   4. If not compatible and requires migration, check if the preceding serializer is valid.
	 *      If yes, use that as the convert deserializer for state migration.
	 *   5. If the preceding serializer is not valid, check if the result came with a convert deserializer.
	 *      If yes, use that for state migration and simply return the result.
	 *   6. If all of above fails, state migration is required but could not be performed; throw exception.
	 *
	 * @param precedingSerializer the preceding serializer used to write the data, null if none could be retrieved
	 * @param dummySerializerClassTag any class tags that identifies the preceding serializer as a dummy placeholder
	 * @param precedingSerializerConfigSnapshot configuration snapshot of the preceding serializer
	 * @param newSerializer the new serializer to ensure compatibility with
	 *
	 * @param <T> Type of the data handled by the serializers
	 * 
	 * @return the final resolved compatibility result
	 */
	@SuppressWarnings("unchecked")
	public static <T> CompatibilityResult<T> resolveCompatibilityResult(
			@Nullable TypeSerializer<?> precedingSerializer,
			Class<?> dummySerializerClassTag,
			TypeSerializerConfigSnapshot precedingSerializerConfigSnapshot,
			TypeSerializer<T> newSerializer) {

		if (precedingSerializerConfigSnapshot != null) {
			CompatibilityResult<T> initialResult = newSerializer.ensureCompatibility(precedingSerializerConfigSnapshot);

			if (!initialResult.isRequiresMigration()) {
				return initialResult;
			} else {
				if (precedingSerializer != null && !(precedingSerializer.getClass().equals(dummySerializerClassTag))) {
					// if the preceding serializer exists and is not a dummy, use
					// that for converting instead of any provided convert deserializer
					return CompatibilityResult.requiresMigration((TypeSerializer<T>) precedingSerializer);
				} else {
					// requires migration (may or may not have a convert deserializer)
					return initialResult;
				}
			}
		} else {
			// if the configuration snapshot of the preceding serializer cannot be provided,
			// we can only simply assume that the new serializer is compatible
			return CompatibilityResult.compatible();
		}
	}

}
