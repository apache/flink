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

package org.apache.flink.state.api.runtime;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

/**
 * A classloader that does not work.
 */
public class NullClassLoader extends ClassLoader {

	static final NullClassLoader INSTANCE = new NullClassLoader();

	private NullClassLoader() {}

	@Override
	public Class<?> loadClass(String name) {
		throw new UnsupportedOperationException(
			"This classloader is only for satisfying api's, " +
				"it should not be used");
	}

	@Override
	public URL getResource(String name) {
		throw new UnsupportedOperationException(
			"This classloader is only for satisfying api's, " +
				"it should not be used");
	}

	@Override
	public Enumeration<URL> getResources(String name) {
		throw new UnsupportedOperationException(
			"This classloader is only for satisfying api's, " +
				"it should not be used");
	}

	@Override
	public InputStream getResourceAsStream(String name) {
		throw new UnsupportedOperationException(
			"This classloader is only for satisfying api's, " +
				"it should not be used");
	}
}
