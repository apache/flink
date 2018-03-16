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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;
import org.apache.flink.util.InstantiationUtil;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * This is a reimplementation of Kryo's {@link com.esotericsoftware.kryo.serializers.JavaSerializer},
 * that additionally makes sure the {@link ObjectInputStream} used for deserialization specifically uses Kryo's
 * registered classloader.
 *
 * Flink maintains this reimplementation due to a known issue with Kryo's {@code JavaSerializer}, in which the wrong
 * classloader may be used for deserialization, leading to {@link ClassNotFoundException}s.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-6025">FLINK-6025</a>
 * @see <a href="https://github.com/EsotericSoftware/kryo/pull/483">Known issue with Kryo's JavaSerializer</a>
 *
 * @param <T> The type to be serialized.
 */
public class JavaSerializer<T> extends Serializer<T> {

	public JavaSerializer() {}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public void write(Kryo kryo, Output output, T o) {
		try {
			ObjectMap graphContext = kryo.getGraphContext();
			ObjectOutputStream objectStream = (ObjectOutputStream)graphContext.get(this);
			if (objectStream == null) {
				objectStream = new ObjectOutputStream(output);
				graphContext.put(this, objectStream);
			}
			objectStream.writeObject(o);
			objectStream.flush();
		} catch (Exception ex) {
			throw new KryoException("Error during Java serialization.", ex);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public T read(Kryo kryo, Input input, Class aClass) {
		try {
			ObjectMap graphContext = kryo.getGraphContext();
			ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);
			if (objectStream == null) {
				// make sure we use Kryo's classloader
				objectStream = new InstantiationUtil.ClassLoaderObjectInputStream(input, kryo.getClassLoader());
				graphContext.put(this, objectStream);
			}
			return (T) objectStream.readObject();
		} catch (Exception ex) {
			throw new KryoException("Error during Java deserialization.", ex);
		}
	}

}
