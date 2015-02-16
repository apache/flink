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

import java.net.InetAddress;
import java.net.ServerSocket;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.NetUtils;
import org.junit.Test;

import scala.Some;
import scala.Tuple2;

public class JobManagerStartupTest {

	@Test
	public void testStartupWithPortInUse() {
		
		ServerSocket portOccupier = null;
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
			Tuple2<String, Object> connection = new Tuple2<String, Object>("localhost", portNum);
			JobManager.runJobManager(new Configuration(), ExecutionMode.CLUSTER(), new Some<Tuple2<String, Object>>(connection));
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
			catch (Throwable t) {}
		}
	}

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
		Tuple2<String, Object> connection = new Tuple2<String, Object>("localhost", portNum);
		Configuration failConfig = new Configuration();
		failConfig.setString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, "/does-not-exist-no-sir");

		try {
			JobManager.runJobManager(failConfig, ExecutionMode.CLUSTER(), new Some<Tuple2<String, Object>>(connection));
			fail("this should fail with an exception");
		}
		catch (Exception e) {
			// expected
		}
	}

}
