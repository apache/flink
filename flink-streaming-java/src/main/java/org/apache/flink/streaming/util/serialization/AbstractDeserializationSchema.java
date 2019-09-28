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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 *
 * <p>This base variant of the deserialization schema produces the type information
 * automatically by extracting it from the generic class arguments.
 *
 * @param <T> The type created by the deserialization schema.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.AbstractDeserializationSchema} instead.
 */
@Deprecated
@PublicEvolving
@SuppressWarnings("deprecation")
public abstract class AbstractDeserializationSchema<T>
		extends org.apache.flink.api.common.serialization.AbstractDeserializationSchema<T>
		implements DeserializationSchema<T> {

	private static final long serialVersionUID = 1L;
}
