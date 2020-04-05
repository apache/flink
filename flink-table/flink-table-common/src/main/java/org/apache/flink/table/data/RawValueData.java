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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.RawType;

/**
 * {@link RawValueData} is a data structure representing data of {@link RawType}
 * in Flink Table/SQL.
 *
 * @param <T> originating class for the raw value
 */
@PublicEvolving
public interface RawValueData<T> {

	/**
	 * Converts a {@link RawValueData} into a Java object, the {@code serializer}
	 * is required because the "raw value" might be in binary format which can be
	 * deserialized by the {@code serializer}.
	 *
	 * <p>Note: the returned Java object may be reused.
	 */
	T toObject(TypeSerializer<T> serializer);

	/**
	 * Converts a {@link RawValueData} into a byte array, the {@code serializer}
	 * is required because the "raw value" might be in Java object format which
	 * can be serialized by the {@code serializer}.
	 *
	 * <p>Note: the returned bytes may be reused.
	 */
	byte[] toBytes(TypeSerializer<T> serializer);

	// ------------------------------------------------------------------------

	/**
	 * Creates a {@link RawValueData} instance from a java object.
	 */
	static <T> RawValueData<T> fromObject(T javaObject) {
		// TODO
		throw new UnsupportedOperationException();
	}

}
