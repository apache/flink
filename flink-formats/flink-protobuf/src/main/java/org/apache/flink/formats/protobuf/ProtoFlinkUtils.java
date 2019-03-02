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

package org.apache.flink.formats.protobuf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.Message;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

/**
 * Utilities for working with protobuf classes in flink.
 */
public class ProtoFlinkUtils {

	/**
	 * Register so that protobuf classes get serialized using protobuf serializers.
	 *
	 * <p>Protobuf serialization is faster as well as bug free compared to trying to use Kryo for
	 * protobuf classes, which is heavy reflection based.
	 */
	public static void registerProtobufSerDeWithKryo(StreamExecutionEnvironment env) {
		try {
			// https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
			env.getConfig()
				.addDefaultKryoSerializer(
					Class.forName("java.util.Collections$UnmodifiableCollection"),
					UnmodifiableCollectionsSerializer.class);
		} catch (ClassNotFoundException e) {
			throw new UnsupportedOperationException(
				"Unable to find class: java.util.Collections$UnmodifiableCollection.");
		}
		env.getConfig().addDefaultKryoSerializer(Message.class, NonLazyProtobufSerializer.class);
	}
}
