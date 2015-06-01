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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.akka.AkkaUtils;


public abstract class AbstractTestBase extends TestBaseUtils {


	protected final Configuration config;

	private final List<File> tempFiles;

	private final FiniteDuration timeout;
	
	protected int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;

	protected int numTaskManagers = DEFAULT_NUM_TASK_MANAGERS;
	
	protected ForkableFlinkMiniCluster executor;
	

	public AbstractTestBase(Configuration config) {
		this.config = config;
		this.tempFiles = new ArrayList<File>();

		timeout = AkkaUtils.getTimeout(config);
	}

	// --------------------------------------------------------------------------------------------
	//  Local Test Cluster Life Cycle
	// --------------------------------------------------------------------------------------------

	public void startCluster() throws Exception{
		this.executor = startCluster(numTaskManagers, taskManagerNumSlots, StreamingMode.BATCH_ONLY, false, true);
	}

	public void stopCluster() throws Exception {
		stopCluster(executor, timeout);

		deleteAllTempFiles();
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
		Files.write(contents, f, Charsets.UTF_8);
		return f.toURI().toString();
	}

	public File createAndRegisterTempFile(String fileName) throws IOException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		File f = new File(baseDir, this.getClass().getName() + "-" + fileName);

		if (f.exists()) {
			deleteRecursively(f);
		}

		File parentToDelete = f;
		while (true) {
			File parent = parentToDelete.getParentFile();
			if (parent == null) {
				throw new IOException("Missed temp dir while traversing parents of a temp file.");
			}
			if (parent.equals(baseDir)) {
				break;
			}
			parentToDelete = parent;
		}

		Files.createParentDirs(f);
		this.tempFiles.add(parentToDelete);
		return f;
	}

	private void deleteAllTempFiles() throws IOException {
		for (File f : this.tempFiles) {
			if (f.exists()) {
				deleteRecursively(f);
			}
		}
	}
}
