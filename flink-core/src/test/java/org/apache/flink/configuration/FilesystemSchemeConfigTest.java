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

package org.apache.flink.configuration;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilesystemSchemeConfigTest {

	@Before
	public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
		IllegalAccessException {
		// reset GlobalConfiguration between tests
		Field instance = GlobalConfiguration.class.getDeclaredField("SINGLETON");
		instance.setAccessible(true);
		instance.set(null, null);
	}
	
	@Test
	public void testExplicitFilesystemScheme() {
		testSettingFilesystemScheme(false, "fs.default-scheme: otherFS://localhost:1234/", true);
	}

	@Test
	public void testSettingFilesystemSchemeInConfiguration() {
		testSettingFilesystemScheme(false, "fs.default-scheme: file:///", false);
	}

	@Test
	public void testUsingDefaultFilesystemScheme() {
		testSettingFilesystemScheme(true, "fs.default-scheme: file:///", false);
	}

	private void testSettingFilesystemScheme(boolean useDefaultScheme,
											String configFileScheme, boolean useExplicitScheme) {
		final File tmpDir = getTmpDir();
		final File confFile = createRandomFile(tmpDir, ".yaml");
		final File testFile = new File(tmpDir.getAbsolutePath() + File.separator + "testing.txt");

		try {
			try {
				final PrintWriter pw1 = new PrintWriter(confFile);
				if(!useDefaultScheme) {
					pw1.println(configFileScheme);
				}
				pw1.close();

				final PrintWriter pwTest = new PrintWriter(testFile);
				pwTest.close();

			} catch (FileNotFoundException e) {
				fail(e.getMessage());
			}

			GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());
			Configuration conf = GlobalConfiguration.getConfiguration();

			try {
				FileSystem.setDefaultScheme(conf);
				String noSchemePath = testFile.toURI().getPath(); // remove the scheme.

				URI uri = new URI(noSchemePath);
				// check if the scheme == null (so that we get the configuration one.
				assertTrue(uri.getScheme() == null);

				// get the filesystem with the default scheme as set in the confFile1
				FileSystem fs = useExplicitScheme ? FileSystem.get(testFile.toURI()) : FileSystem.get(uri);
				assertTrue(fs.exists(new Path(noSchemePath)));

			} catch (IOException e) {
				fail(e.getMessage());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		} finally {
			try {
				// clear the default scheme set in the FileSystem class.
				// we do it through reflection to avoid creating a publicly
				// accessible method, which could also be wrongly used by users.

				Field f = FileSystem.class.getDeclaredField("defaultScheme");
				f.setAccessible(true);
				f.set(null, null);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			}
			confFile.delete();
			testFile.delete();
			tmpDir.delete();
		}
	}

	private File getTmpDir() {
		File tmpDir = new File(CommonTestUtils.getTempDir() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + File.separator);
		tmpDir.mkdirs();

		return tmpDir;
	}

	private File createRandomFile(File path, String suffix) {
		return new File(path.getAbsolutePath() + File.separator + CommonTestUtils.getRandomDirectoryName() + suffix);
	}
}
