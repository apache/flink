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

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

/**
 * Function to extract a key from a given object.
 *
 * @param <T> type of the element from which we extract the key.
 */
@FunctionalInterface
public interface KeyExtractorFunction<T> {

	KeyExtractorFunction<? extends Keyed<?>> FOR_KEYED_OBJECTS = new KeyExtractorFunction<Keyed<?>>() {
		@Nonnull
		@Override
		public Object extractKeyFromElement(@Nonnull Keyed<?> element) {
			return element.getKey();
		}
	};

	/**
	 * Returns the key for the given element by which the key-group can be computed.
	 */
	@Nonnull
	Object extractKeyFromElement(@Nonnull T element);

	@SuppressWarnings("unchecked")
	static <T extends Keyed<?>> KeyExtractorFunction<T> forKeyedObjects() {
		return (KeyExtractorFunction<T>) FOR_KEYED_OBJECTS;
	}
}
