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

package org.apache.flink.migration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.migration.v0.SavepointV0;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to deserialize legacy classes for migration.
 */
@PublicEvolving
public final class MigrationInstantiationUtil {

	private static final Map<Integer, Map<String, String>> MigrationMappings = new HashMap<Integer, Map<String, String>>() {{
		put(SavepointV0.VERSION, SavepointV0.MigrationMapping);
	}};


	public static class ClassLoaderObjectInputStream extends InstantiationUtil.ClassLoaderObjectInputStream {

		private static final String ARRAY_PREFIX = "[L";
		private static final String ARRAY_SUFFIX = ";";

		private final Map<String, String> migrationMapping;

		public ClassLoaderObjectInputStream(int version, InputStream in, ClassLoader classLoader) throws IOException {
			super(in, classLoader);

			if (!MigrationMappings.containsKey(version)) {
				throw new IllegalStateException("Unknown migration version: " + version);
			}

			this.migrationMapping = MigrationMappings.get(version);
		}

		@Override
		protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
			ObjectStreamClass objectStreamClass = super.readClassDescriptor();

			String className = objectStreamClass.getName();

			String arrayPrefix = "";
			String arraySuffix = "";
			while (className.startsWith(ARRAY_PREFIX) && className.endsWith(ARRAY_SUFFIX)) {
				arrayPrefix += ARRAY_PREFIX;
				arraySuffix += ARRAY_SUFFIX;
				className = className.substring(ARRAY_PREFIX.length(), className.length() - ARRAY_SUFFIX.length());
			}

			String migratedClassName = migrationMapping.get(className);
			if (migratedClassName != null) {
				migratedClassName = arrayPrefix + migratedClassName + arraySuffix;

				Class<?> clazz = classLoader == null ?
					Class.forName(migratedClassName) :
					Class.forName(migratedClassName, false, classLoader);

				objectStreamClass = ObjectStreamClass.lookup(clazz);
			}

			return objectStreamClass;
		}
	}

	public static <T> T deserializeObject(int version, byte[] bytes, ClassLoader cl) throws IOException, ClassNotFoundException {
		return deserializeObject(version, new ByteArrayInputStream(bytes), cl);
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserializeObject(int version, InputStream in, ClassLoader cl) throws IOException, ClassNotFoundException {
		final ClassLoader old = Thread.currentThread().getContextClassLoader();
		try (ObjectInputStream oois = new ClassLoaderObjectInputStream(version, in, cl)) {
			Thread.currentThread().setContextClassLoader(cl);
			return (T) oois.readObject();
		} finally {
			Thread.currentThread().setContextClassLoader(old);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MigrationInstantiationUtil() {
		throw new IllegalAccessError();
	}

}
