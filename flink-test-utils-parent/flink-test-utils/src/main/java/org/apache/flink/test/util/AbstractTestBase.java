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

package org.apache.flink.test.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.util.FileUtils;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import scala.concurrent.duration.FiniteDuration;

/**
 * A base class for tests that run test programs in a Flink mini cluster.
 */
public abstract class AbstractTestBase extends TestBaseUtils {

	/** Configuration to start the testing cluster with. */
	protected final Configuration config;

	private final FiniteDuration timeout;

	protected int taskManagerNumSlots = 1;

	protected int numTaskManagers = 1;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	/** The mini cluster that runs the test programs. */
	protected LocalFlinkMiniCluster executor;

	public AbstractTestBase(Configuration config) {
		this.config = Objects.requireNonNull(config);

		timeout = AkkaUtils.getTimeout(config);
	}

	// --------------------------------------------------------------------------------------------
	//  Local Test Cluster Life Cycle
	// --------------------------------------------------------------------------------------------

	public void startCluster() throws Exception {
		this.executor = startCluster(
			numTaskManagers,
			taskManagerNumSlots,
			false,
			false,
			true);
	}

	public void stopCluster() throws Exception {
		stopCluster(executor, timeout);
	}

	//------------------
	// Accessors
	//------------------

	public int getTaskManagerNumSlots() {
		return taskManagerNumSlots;
	}

	public void setTaskManagerNumSlots(int taskManagerNumSlots) {
		this.taskManagerNumSlots = taskManagerNumSlots;
	}

	public int getNumTaskManagers() {
		return numTaskManagers;
	}

	public void setNumTaskManagers(int numTaskManagers) {
		this.numTaskManagers = numTaskManagers;
	}

	// --------------------------------------------------------------------------------------------
	//  Temporary File Utilities
	// --------------------------------------------------------------------------------------------

	public String getTempDirPath(String dirName) throws IOException {
		File f = createAndRegisterTempFile(dirName);
		return f.toURI().toString();
	}

	public String getTempFilePath(String fileName) throws IOException {
		File f = createAndRegisterTempFile(fileName);
		return f.toURI().toString();
	}

	public String createTempFile(String fileName, String contents) throws IOException {
		File f = createAndRegisterTempFile(fileName);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		f.createNewFile();
		FileUtils.writeFileUtf8(f, contents);
		return f.toURI().toString();
	}

	public File createAndRegisterTempFile(String fileName) throws IOException {
		return new File(TEMPORARY_FOLDER.newFolder(), fileName);
	}
}
