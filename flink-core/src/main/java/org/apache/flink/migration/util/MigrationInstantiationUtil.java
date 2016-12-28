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

package org.apache.flink.migration.util;

import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Utility class to deserialize legacy classes for migration.
 */
public final class MigrationInstantiationUtil {

	public static class ClassLoaderObjectInputStream extends InstantiationUtil.ClassLoaderObjectInputStream {

		private static final String FLINK_BASE_PACKAGE = "apache.flink.";
		private static final String FLINK_MIGRATION_PACKAGE = "apache.flink.migration.";

		public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
			super(in, classLoader);
		}

		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
			String className = desc.getName();
			if (className.contains(FLINK_BASE_PACKAGE)) {
				className = className.replace(FLINK_BASE_PACKAGE, FLINK_MIGRATION_PACKAGE);
				try {
					return classLoader != null ? Class.forName(className, false, classLoader) : Class.forName(className);
				} catch (ClassNotFoundException ignored) {

				}
			}
			return super.resolveClass(desc);
		}
	}
	
	public static <T> T deserializeObject(byte[] bytes, ClassLoader cl) throws IOException, ClassNotFoundException {
		return deserializeObject(new ByteArrayInputStream(bytes), cl);
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserializeObject(InputStream in, ClassLoader cl) throws IOException, ClassNotFoundException {
		final ClassLoader old = Thread.currentThread().getContextClassLoader();
		try (ObjectInputStream oois = new ClassLoaderObjectInputStream(in, cl)) {
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
