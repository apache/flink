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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlobKey.class)
public class BlobUtilsTest {

	@Test(expected = Exception.class)
	public void testExceptionOnCreateStorageDirectoryFailure() {

		// Configure a non existing directory
		Configuration config = new Configuration();
		config.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, "/cannot-create-this");

		GlobalConfiguration.includeConfiguration(config);

		// Should throw an Exception
		BlobUtils.initStorageDirectory();
	}

	@Test(expected = Exception.class)
	public void testExceptionOnCreateCacheDirectoryFailure() {
		// Should throw an Exception
		BlobUtils.getStorageLocation(new File("/cannot-create-this"), mock(BlobKey.class));
	}
}
