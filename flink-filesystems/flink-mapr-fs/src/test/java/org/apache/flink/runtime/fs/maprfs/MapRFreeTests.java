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

package org.apache.flink.runtime.fs.maprfs;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class with tests that require to be run in a MapR/Hadoop-free environment,
 * to test proper error handling when no Hadoop classes are available.
 *
 * <p>This class must be dynamically loaded in a MapR/Hadoop-free class loader.
 */
// this class is only instantiated via reflection
@SuppressWarnings("unused")
public class MapRFreeTests {

	public static void test() throws Exception {
		// make sure no MapR or Hadoop FS classes are in the classpath
		try {
			Class.forName("com.mapr.fs.MapRFileSystem");
			fail("Cannot run test when MapR classes are in the classpath");
		}
		catch (ClassNotFoundException ignored) {}

		try {
			Class.forName("org.apache.hadoop.fs.FileSystem");
			fail("Cannot run test when Hadoop classes are in the classpath");
		}
		catch (ClassNotFoundException ignored) {}

		try {
			Class.forName("org.apache.hadoop.conf.Configuration");
			fail("Cannot run test when Hadoop classes are in the classpath");
		}
		catch (ClassNotFoundException ignored) {}

		// this method should complete without a linkage error
		final MapRFsFactory factory = new MapRFsFactory();

		// this method should also complete without a linkage error
		factory.configure(new Configuration());

		try {
			factory.create(new URI("maprfs://somehost:9000/root/dir"));
			fail("This statement should fail with an exception");
		}
		catch (IOException e) {
			assertTrue(e.getMessage().contains("MapR"));
			assertTrue(e.getMessage().contains("classpath"));
		}
	}
}
