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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.junit.AfterClass;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode.*;
import static org.mockito.Mockito.mock;

public class SpillableSubpartitionTest extends SubpartitionTestBase {

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	/** Asynchronous I/O manager */
	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdown() {
		executorService.shutdownNow();
		ioManager.shutdown();
	}

	@Override
	ResultSubpartition createSubpartition() {
		return new SpillableSubpartition(0, mock(ResultPartition.class), ioManager, SYNC);
	}
}
