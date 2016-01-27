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

package org.apache.flink.runtime.jobmanager;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.ServerSocket;

import com.google.common.io.Files;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NetUtils;

import org.apache.flink.util.OperatingSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that verify the startup behavior of the JobManager in failure
 * situations, when the JobManager cannot be started.
 */
public class JobManagerStartupTest {

	private final static String DOES_NOT_EXISTS_NO_SIR = "does-not-exist-no-sir";

	private File blobStorageDirectory;

	@Before
	public void before() {
		// Prepare test directory
		blobStorageDirectory = Files.createTempDir();

		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		assertTrue(blobStorageDirectory.setExecutable(true, false));
		assertTrue(blobStorageDirectory.setReadable(true, false));
		assertTrue(blobStorageDirectory.setWritable(false, false));
	}

	@After
	public void after() {
		// Cleanup test directory
		assertTrue(blobStorageDirectory.delete());
	}

	/**
	 * Verifies that the JobManager fails fast (and with expressive error message)
	 * when the port to listen is already in use.
	 */
	@Test
	public void testStartupWithPortInUse() {
		
		ServerSocket portOccupier;
		final int portNum;
		
		try {
			portNum = NetUtils.getAvailablePort();
			portOccupier = new ServerSocket(portNum, 10, InetAddress.getByName("localhost"));
		}
		catch (Throwable t) {
			// could not find free port, or open a connection there
			return;
		}
		
		try {
			JobManager.runJobManager(new Configuration(), JobManagerMode.CLUSTER, "localhost", portNum);
			fail("this should throw an exception");
		}
		catch (Exception e) {
			// expected
			if(!e.getMessage().contains("Address already in use")) {
				e.printStackTrace();
				fail("Received wrong exception");
			}
		}
		finally {
			try {
				portOccupier.close();
			}
			catch (Throwable t) {
				// ignore
			}
		}
	}

	/**
	 * Verifies that the JobManager fails fast (and with expressive error message)
	 * when one of its components (here the BLOB server) fails to start properly.
	 */
	@Test
	public void testJobManagerStartupFails() {
		final int portNum;
		try {
			portNum = NetUtils.getAvailablePort();
		}
		catch (Throwable t) {
			// skip test if we cannot find a free port
			return;
		}
		Configuration failConfig = new Configuration();
		String nonExistDirectory = new File(blobStorageDirectory, DOES_NOT_EXISTS_NO_SIR).getAbsolutePath();
		failConfig.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, nonExistDirectory);

		try {
			JobManager.runJobManager(failConfig, JobManagerMode.CLUSTER, "localhost", portNum);
			fail("this should fail with an exception");
		}
		catch (Exception e) {
			// expected
		}
	}
}
