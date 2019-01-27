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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({YarnLocalResultPartitionResolver.class})
public class YarnLocalResultPartitionResolverTest {
	private static final Logger LOG = LoggerFactory.getLogger(YarnLocalResultPartitionResolverTest.class);

	private static final long UNCONSUMED_PARTITION_TTL = 600000;

	private static final long UNFINISHED_PARTITION_TTL = 300000;

	private static final long CUSTOMIZED_CONSUMED_PARTITION_TTL = 120000;

	private static final long CUSTOMIZED_PARTIAL_CONSUMED_PARTITION_TTL = 130000;

	private static final long CUSTOMIZED_UNCONSUMED_PARTITION_TTL = 160000;

	private static final long CUSTOMIZED_UNFINISHED_PARTITION_TTL = 150000;

	private static final ConfigOption<Boolean> ENABLE_CUSTOMIZED_TTL = ConfigOptions.key("enable-customized-ttl").defaultValue(false);

	private static final FileSystem FILE_SYSTEM = FileSystem.getLocalFileSystem();

	private final ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration =
		mock(ExternalBlockShuffleServiceConfiguration.class);

	private YarnLocalResultPartitionResolver resultPartitionResolver;

	private final int localDirCnt = 3;

	private String testRootDir;

	private ConcurrentHashMap<String, String> appIdToUser;

	private HashMap<ResultPartitionID, String> resultPartitionIDToAppId;

	private HashMap<ResultPartitionID, Tuple2<String, String>> resultPartitionIDToLocalDir;

	enum ResultPartitionState {
		UNDEFINED,
		UNFINISHED_NO_CONFIG,
		UNFINISHED_HAS_CONFIG,
		UNCONSUMED,
		CONSUMED
	}

	/** Deque should be used as a stack due to case design. */
	private HashMap<ResultPartitionState, Deque<ResultPartitionID>> stateToResultPartitionIDs;

	private Set<ResultPartitionID> removedResultPartitionIDs;

	private Set<ResultPartitionID> toBeRemovedResultPartitionIDs;

	@Before
	public void setup() throws IOException {
		appIdToUser = new ConcurrentHashMap<>();
		resultPartitionIDToAppId = new HashMap<>();
		resultPartitionIDToLocalDir = new HashMap<>();
		stateToResultPartitionIDs = new HashMap<ResultPartitionState, Deque<ResultPartitionID>>() {{
			for (ResultPartitionState value : ResultPartitionState.values()) {
				put(value, new ArrayDeque<>());
			}
		}};
		removedResultPartitionIDs = new HashSet<>();
		toBeRemovedResultPartitionIDs = new HashSet<>();

		Configuration configuration = new Configuration();
		when(externalBlockShuffleServiceConfiguration.getConfiguration()).thenReturn(configuration);
		when(externalBlockShuffleServiceConfiguration.getFileSystem()).thenReturn(FILE_SYSTEM);
		when(externalBlockShuffleServiceConfiguration.getDiskScanIntervalInMS()).thenReturn(3600000L);
		when(externalBlockShuffleServiceConfiguration.getDefaultUnconsumedPartitionTTL()).thenReturn(UNCONSUMED_PARTITION_TTL);
		when(externalBlockShuffleServiceConfiguration.getDefaultUnfinishedPartitionTTL()).thenReturn(UNFINISHED_PARTITION_TTL);

		checkArgument( UNFINISHED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"UNFINISHED_PARTITION_TTL should be less than UNFINISHED_PARTITION_TTL to test recycling");

		checkArgument( CUSTOMIZED_UNFINISHED_PARTITION_TTL < UNFINISHED_PARTITION_TTL,
			"The customized UNFINISHED_PARTITION_TTL should be less than the default one to test the customized TTL");
		checkArgument( CUSTOMIZED_UNCONSUMED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"The customized UNCONSUMED_PARTITION_TTL should be less than the default one to test the customized TTL");
		checkArgument( CUSTOMIZED_UNFINISHED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"The customized CUSTOMIZED_UNFINISHED_PARTITION_TTL should be less than the customized UNCONSUMED_PARTITION_TTL to " +
				"test the customized TTL");

		this.testRootDir = System.getProperty("java.io.tmpdir");
		if (!System.getProperty("java.io.tmpdir").endsWith("/")) {
			this.testRootDir += "/";
		}
		this.testRootDir += "yarn_shuffle_test_" + UUID.randomUUID().toString() + "/";
		FILE_SYSTEM.mkdirs(new Path(testRootDir));
		assertTrue("Fail to create testRootDir: " + testRootDir, FILE_SYSTEM.exists(new Path(testRootDir)));

		String localDirPrefix = "localDir";
		Map<String, String> dirToDiskType = new HashMap<>();
		for (int i = 0; i < localDirCnt; i++) {
			String localDir = testRootDir + localDirPrefix + i + "/";
			dirToDiskType.put(localDir, "SSD");
			FILE_SYSTEM.mkdirs(new Path(localDir));
			assertTrue("Fail to create local dir: " + localDir, FILE_SYSTEM.exists(new Path(localDir)));
		}
		when(externalBlockShuffleServiceConfiguration.getDirToDiskType()).thenReturn(dirToDiskType);

		mockStatic(System.class);
		// Mock container-executor.
		when(System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key())).thenReturn(testRootDir);
		String containerExecutorPath = testRootDir + "bin/container-executor";
		configuration.setString(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, containerExecutorPath);
		FILE_SYSTEM.create(new Path(containerExecutorPath), FileSystem.WriteMode.OVERWRITE);
		assertTrue("Fail to mock container-executor: " + containerExecutorPath,
			FILE_SYSTEM.exists(new Path(containerExecutorPath)));

		createYarnLocalResultPartitionResolver();

		LOG.debug("Configurations for YarnLocalResultPartitionResolverTest:\n\tTest root dir: " + testRootDir);
	}

	@After
	public void tearDown() {
		if (resultPartitionResolver != null) {
			resultPartitionResolver.stop();
		}
	}

	@Rule
	public TestWatcher watchman = new TestWatcher() {
		@Override
		protected void succeeded(Description description) {
			// Do recycle after test cases pass.
			try {
				Path testRootDirPath = new Path(testRootDir);
				if (FILE_SYSTEM.exists(testRootDirPath)) {
					FILE_SYSTEM.delete(testRootDirPath, true);
				}
			} catch (IOException e) {
				// Do nothing
			}

			super.succeeded(description);
		}

		@Override
		protected void failed(Throwable e, Description description) {
			// Leave result partition directories for debugging.
			super.failed(e, description);
		}
	};

	@Test
	public void testBasicProcess() {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});
		assertEquals(appIdToUser, resultPartitionResolver.appIdToUser);

		// 2. Upstream tasks start to generate external result partitions.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 6, UNCONSUMED: 0, CONSUMED: 0, REMOVED: 0}
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNFINISHED_HAS_CONFIG, resultPartitionCnt);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 3. One upstream task finishes writing.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 4, UNCONSUMED: 2, CONSUMED: 0, REMOVED: 0}
		changeResultPartitionState(ResultPartitionState.UNFINISHED_HAS_CONFIG, ResultPartitionState.UNCONSUMED, 2);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 4. One upstream task finishes writing and becomes consumed before disk scan.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 3, UNCONSUMED: 2, CONSUMED: 1, REMOVED: 0}
		changeResultPartitionState(ResultPartitionState.UNFINISHED_HAS_CONFIG, ResultPartitionState.CONSUMED, 1);

		// Knows this result partition is consumable by result partition request.
		validateResultPartitionMetaByState();

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 5. A new upstream task starts to generate external result partition, quickly becomes finished
		//    then becomes consumed before disk scan.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 3, UNCONSUMED: 2, CONSUMED: 2, REMOVED: 0}
		createResultPartitionIDs(1);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.CONSUMED, 1);

		// Knows this result partition is consumable by result partition request.
		validateResultPartitionMetaByState();

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 6. A new upstream task starts to generate external result partition, quickly becomes finished.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 3, UNCONSUMED: 3, CONSUMED: 2, REMOVED: 0}
		createResultPartitionIDs(1);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNCONSUMED, 1);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 7. One unconsumed result partition becomes consumed.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 3, UNCONSUMED: 2, CONSUMED: 3, REMOVED: 0}
		changeResultPartitionState(ResultPartitionState.UNCONSUMED, ResultPartitionState.CONSUMED, 1);

		// Knows this result partition is consumable by result partition request.
		validateResultPartitionMetaByState();

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 8. Recycle a consumed result partition decided by ExternalResultPartitionManager.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 3, UNCONSUMED: 2, CONSUMED: 2, REMOVED: 1}
		triggerRecycleConsumedResultPartition(1);

		validateResultPartitionMetaByState();

		removedResultPartitionIDs.addAll(toBeRemovedResultPartitionIDs);
		toBeRemovedResultPartitionIDs.clear();

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 9. Remove three different kinds of result partition directories.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 2, UNCONSUMED: 1, CONSUMED: 1, REMOVED: 4}
		removeResultPartitionByState(ResultPartitionState.UNFINISHED_HAS_CONFIG, 1);
		removeResultPartitionByState(ResultPartitionState.UNCONSUMED, 1);
		removeResultPartitionByState(ResultPartitionState.CONSUMED, 1);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 10. Stops applications one by one.
		Map<String, Set<ResultPartitionID>> appIdToResultPartitionIDs = aggregateRemainingResultPartitionByAppId();
		LOG.debug("RemainingResultPartitions: " + appIdToResultPartitionIDs);
		for (Map.Entry<String, Set<ResultPartitionID>> entry : appIdToResultPartitionIDs.entrySet()) {
			String appId = entry.getKey();
			Set<ResultPartitionID> resultPartitionIDS = entry.getValue();

			LOG.debug("StopApplication: " + appId + ", resultPartitions: " + resultPartitionIDS);
			Set<ResultPartitionID> remainingResultPartitions = resultPartitionResolver.stopApplication(appId);
			assertEquals(resultPartitionIDS, remainingResultPartitions);
			assertTrue(!resultPartitionResolver.appIdToUser.contains(appId));
			remainingResultPartitions.forEach(resultPartitionID -> {
				resultPartitionResolver.recycleResultPartition(resultPartitionID);
			});
			removeResultPartitionById(resultPartitionIDS);
			when(System.currentTimeMillis()).thenReturn(++currTime);
			triggerDiskScan();
			validateResultPartitionMetaByState();
		}
	}

	@Test
	public void testRecycleByTTL() {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});
		assertEquals(appIdToUser, resultPartitionResolver.appIdToUser);

		// 2. Prepare three different types of external result partition.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 2, UNCONSUMED: 2, CONSUMED: 2, REMOVED: 0}
		when(System.currentTimeMillis()).thenReturn(++currTime);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNFINISHED_HAS_CONFIG, 2);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNCONSUMED, 2);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.CONSUMED, 2);

		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 3. Trigger UNFINISHED_PARTITION_TTL_TIMEOUT since its TTL is shorter.
		when(System.currentTimeMillis()).thenCallRealMethod();
		long realCurrTime = System.currentTimeMillis();
		when(System.currentTimeMillis()).thenReturn(realCurrTime + UNFINISHED_PARTITION_TTL);

		triggerDiskScan();
		Set<ResultPartitionID> unfinishedResultPartitionIDs = pollResultPartitionIDS(ResultPartitionState.UNFINISHED_HAS_CONFIG, 2);
		removedResultPartitionIDs.addAll(unfinishedResultPartitionIDs);
		validateResultPartitionMetaByState();

		// 4. Trigger UNCONSUMED_PARTITION_TTL_TIMEOUT.
		when(System.currentTimeMillis()).thenReturn(realCurrTime + UNCONSUMED_PARTITION_TTL);

		triggerDiskScan();
		Set<ResultPartitionID> unconsumedResultPartitionIDs = pollResultPartitionIDS(ResultPartitionState.UNCONSUMED, 2);
		removedResultPartitionIDs.addAll(unconsumedResultPartitionIDs);
		validateResultPartitionMetaByState();

		// 5. Consumed result partition will not be timed out in YarnLocalResultPartitionResolver.
		when(System.currentTimeMillis()).thenReturn(Long.MAX_VALUE);

		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 6. Recycle consumed result partitions.
		triggerRecycleConsumedResultPartition(2);

		validateResultPartitionMetaByState();

		removedResultPartitionIDs.addAll(toBeRemovedResultPartitionIDs);
		toBeRemovedResultPartitionIDs.clear();

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 7. Stops applications one by one.
		Map<String, Set<ResultPartitionID>> appIdToResultPartitionIDs = aggregateRemainingResultPartitionByAppId();
		LOG.debug("RemainingResultPartitions: " + appIdToResultPartitionIDs);
		for (Map.Entry<String, Set<ResultPartitionID>> entry : appIdToResultPartitionIDs.entrySet()) {
			String appId = entry.getKey();
			Set<ResultPartitionID> resultPartitionIDS = entry.getValue();

			LOG.debug("StopApplication: " + appId + ", resultPartitions: " + resultPartitionIDS);
			Set<ResultPartitionID> remainingResultPartitions = resultPartitionResolver.stopApplication(appId);
			assertEquals(resultPartitionIDS, remainingResultPartitions);
			assertTrue(!resultPartitionResolver.appIdToUser.contains(appId));
			remainingResultPartitions.forEach(resultPartitionID -> {
				resultPartitionResolver.recycleResultPartition(resultPartitionID);
			});
			removeResultPartitionById(resultPartitionIDS);
			when(System.currentTimeMillis()).thenReturn(++currTime);
			triggerDiskScan();
			validateResultPartitionMetaByState();
		}
	}

	@Test
	public void testCustomizedTTL() {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;

		Configuration configuration = new Configuration();
		configuration.setBoolean(ENABLE_CUSTOMIZED_TTL, true);

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});

		// 2. Prepare the external result partition.
		// expect state: {UNDEFINED: 0, UNFINISHED_NO_CONFIG: 6, UNCONSUMED: 0, CONSUMED: 0, REMOVED: 0}
		when(System.currentTimeMillis()).thenReturn(++currTime);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNFINISHED_NO_CONFIG, resultPartitionCnt, configuration);
		triggerDiskScan();
		validateResultPartitionMetaByState(configuration);

		// 3. Test retrieving the result partitions before recycling. The returned fileInfo objects should
		// have the customized TTL.
		when(System.currentTimeMillis()).thenCallRealMethod();
		long realCurrTime = System.currentTimeMillis();
		when(System.currentTimeMillis()).thenReturn(realCurrTime + 1);

		changeResultPartitionState(ResultPartitionState.UNFINISHED_NO_CONFIG, ResultPartitionState.UNCONSUMED, 2, configuration);
		triggerDiskScan();

		Set<ResultPartitionID> unconsumedResultPartitionIDs = pollResultPartitionIDS(ResultPartitionState.UNCONSUMED, 2);
		unconsumedResultPartitionIDs.forEach(resultPartitionID -> {
			try {
				LocalResultPartitionResolver.ResultPartitionFileInfo fileInfo = resultPartitionResolver.getResultPartitionDir(resultPartitionID);

				assertEquals(CUSTOMIZED_CONSUMED_PARTITION_TTL, fileInfo.getConsumedPartitionTTL());
				assertEquals(CUSTOMIZED_PARTIAL_CONSUMED_PARTITION_TTL, fileInfo.getPartialConsumedPartitionTTL());
			} catch (IOException e) {
				fail("Failed to getResultPartitionDir due to " + e.getMessage());
			}
		});

		// 4. Test recycling the unfinished result partitions due to customized UNFINISHED_PARTITION_TTL. The
		// customized config should be smaller than the default one, thus the customized config works if they get
		// recycled indeed.
		when(System.currentTimeMillis()).thenReturn(realCurrTime + 2);
		changeResultPartitionState(ResultPartitionState.UNFINISHED_NO_CONFIG, ResultPartitionState.UNFINISHED_HAS_CONFIG, 2, configuration);

		when(System.currentTimeMillis()).thenCallRealMethod();
		realCurrTime = System.currentTimeMillis();
		when(System.currentTimeMillis()).thenReturn(realCurrTime + CUSTOMIZED_UNFINISHED_PARTITION_TTL);

		triggerDiskScan();
		Set<ResultPartitionID> unfinishedResultPartitionIDs = pollResultPartitionIDS(ResultPartitionState.UNFINISHED_HAS_CONFIG, 2);
		removedResultPartitionIDs.addAll(unfinishedResultPartitionIDs);
		validateResultPartitionMetaByState();

		// 5. Test recycling the unconsumed result partitions due to customized UNCONSUMED_PARTITION_TTL. The
		// customized config should be smaller than the default one, thus the customized config works if they get
		// recycled indeed.
		changeResultPartitionState(ResultPartitionState.UNFINISHED_NO_CONFIG, ResultPartitionState.UNCONSUMED, 2, configuration);

		when(System.currentTimeMillis()).thenCallRealMethod();
		realCurrTime = System.currentTimeMillis();
		when(System.currentTimeMillis()).thenReturn(realCurrTime + CUSTOMIZED_UNCONSUMED_PARTITION_TTL);

		triggerDiskScan();
		unconsumedResultPartitionIDs = pollResultPartitionIDS(ResultPartitionState.UNCONSUMED, 2);
		removedResultPartitionIDs.addAll(unconsumedResultPartitionIDs);
		validateResultPartitionMetaByState();
	}

	@Test
	public void testGetResultPartitionDir() {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});
		assertEquals(appIdToUser, resultPartitionResolver.appIdToUser);

		// 2. Prepares three different types of external result partition.
		// expect state: {UNDEFINED: 0, UNFINISHED_HAS_CONFIG: 2, UNCONSUMED: 2, CONSUMED: 2, REMOVED: 0}
		when(System.currentTimeMillis()).thenReturn(++currTime);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNFINISHED_HAS_CONFIG, 2);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNCONSUMED, 2);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.CONSUMED, 2);

		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 3. Searches an unknown result partition.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		try {
			ResultPartitionID newResultPartitionID = new ResultPartitionID();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(newResultPartitionID);
			assertTrue("Expect PartitionNotFoundException to be thrown out.", false);
		} catch (PartitionNotFoundException e){
			// Do nothing.
		} catch (IOException e) {
			assertTrue("Unexpected IOException", false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 4. Searches an unfinished result partition.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		try {
			ResultPartitionID unfinishedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_HAS_CONFIG).getLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(unfinishedResultPartitionID);
			assertTrue("Expect PartitionNotFoundException to be thrown out.", false);
		} catch (PartitionNotFoundException e) {
			// Do nothing.
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 5. Searches an unfinished result partition while it's unknown to the resolver.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		createResultPartitionIDs(1);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNFINISHED_HAS_CONFIG, 1);
		try {
			ResultPartitionID unfinishedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_HAS_CONFIG).getLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(unfinishedResultPartitionID);
			assertTrue("Expect PartitionNotFoundException to be thrown out.", false);
		} catch (PartitionNotFoundException e) {
			// Do nothing.
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 6. Searches an unconsumed result partition.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		try {
			ResultPartitionID unconsumedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.UNCONSUMED).pollLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(unconsumedResultPartitionID);
			assertEquals(resultPartitionIDToLocalDir.get(unconsumedResultPartitionID).f0, descriptor.getRootDir());
			assertEquals(resultPartitionIDToLocalDir.get(unconsumedResultPartitionID).f1, descriptor.getPartitionDir());
			stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).offerLast(unconsumedResultPartitionID);
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 7. Searches an unconsumed result partition while it's unknown to the resolver.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		createResultPartitionIDs(1);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.UNCONSUMED, 1);
		try {
			ResultPartitionID unconsumedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.UNCONSUMED).pollLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(unconsumedResultPartitionID);
			assertEquals(resultPartitionIDToLocalDir.get(unconsumedResultPartitionID).f0, descriptor.getRootDir());
			assertEquals(resultPartitionIDToLocalDir.get(unconsumedResultPartitionID).f1, descriptor.getPartitionDir());
			stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).offerLast(unconsumedResultPartitionID);
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 8. Searches an consumed result partition.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		try {
			ResultPartitionID consumedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).getLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(consumedResultPartitionID);
			assertEquals(resultPartitionIDToLocalDir.get(consumedResultPartitionID).f0, descriptor.getRootDir());
			assertEquals(resultPartitionIDToLocalDir.get(consumedResultPartitionID).f1, descriptor.getPartitionDir());
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();

		// 9. Searches an consumed result partition while it's unknown to the resolver.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		createResultPartitionIDs(1);
		changeResultPartitionState(ResultPartitionState.UNDEFINED, ResultPartitionState.CONSUMED, 1);
		try {
			ResultPartitionID consumedResultPartitionID =
				stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).getLast();
			LocalResultPartitionResolver.ResultPartitionFileInfo descriptor =
				resultPartitionResolver.getResultPartitionDir(consumedResultPartitionID);
			assertEquals(resultPartitionIDToLocalDir.get(consumedResultPartitionID).f0, descriptor.getRootDir());
			assertEquals(resultPartitionIDToLocalDir.get(consumedResultPartitionID).f1, descriptor.getPartitionDir());
		} catch (IOException e) {
			assertTrue("Unexpected exception: " + e.getMessage(), false);
		}

		validateResultPartitionMetaByState();
		triggerDiskScan();
		validateResultPartitionMetaByState();
	}

	// ************************************** Test Utilities ********************************************/

	private void createYarnLocalResultPartitionResolver() {
		resultPartitionResolver = spy(new YarnLocalResultPartitionResolver(externalBlockShuffleServiceConfiguration));

		// Mock remove method because we use container-executor provided by yarn to deal with linux privilege operations.
		doAnswer(invocation -> {
			Path partitionDir = invocation.getArgumentAt(0, Path.class);
			String recycleReason = invocation.getArgumentAt(1, String.class);
			long lastActiveTime = invocation.getArgumentAt(2, long.class);
			boolean printLog = invocation.getArgumentAt(3, boolean.class);

			try {
				FILE_SYSTEM.delete(partitionDir, true);
				assertTrue("Fail to delete result partition dir " + partitionDir, !FILE_SYSTEM.exists(partitionDir));
				LOG.debug("Delete partition's directory: {}, reason: {}, lastActiveTime: {}, printLog: {}",
					partitionDir, recycleReason, lastActiveTime, printLog);
			} catch (IOException e) {
				assertTrue("Caught exception when deleting result partition dir " + partitionDir
					+ ", exception: " + e.getMessage(), false);
			}
			return null;
		}).when(resultPartitionResolver).removeResultPartition(
			any(Path.class), any(String.class), any(long.class), any(boolean.class));
	}

	void triggerDiskScan() {
		resultPartitionResolver.doDiskScan();
	}

	private void generateAppIdToUser(int userCnt, int appCnt) {
		for (int i = 0; i < appCnt; i++) {
			String user = "user" + (i % userCnt);
			String appId = "flinkStreaming" + i;
			appIdToUser.put(appId, user);

			// Prepare app's local dirs
			String relativeAppDir = YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId);
			externalBlockShuffleServiceConfiguration.getDirToDiskType().keySet().forEach(localDir -> {
				Path appLocalDir = new Path(localDir + relativeAppDir);
				try {
					FILE_SYSTEM.mkdirs(appLocalDir);
					assertTrue("Fail to mkdir for appLocalDir " + appLocalDir, FILE_SYSTEM.exists(appLocalDir));
				} catch (IOException e) {
					assertTrue("Caught except when mkdir for appLocalDir " + appLocalDir, false);
				}
			});

		}
	}

	private void createResultPartitionIDs(int resultPartitionCnt) {
		String[] appArray = appIdToUser.keySet().toArray(new String[appIdToUser.size()]);
		for (int i = 0; i < resultPartitionCnt; i++) {
			ResultPartitionID resultPartitionID = new ResultPartitionID();
			resultPartitionIDToAppId.put(resultPartitionID, appArray[i % appArray.length]);
			stateToResultPartitionIDs.get(ResultPartitionState.UNDEFINED).offerLast(resultPartitionID);
		}
	}

	private Set<ResultPartitionID> pollResultPartitionIDS(ResultPartitionState state, int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = new HashSet();
		Deque<ResultPartitionID> totalResultPartitionIDS = stateToResultPartitionIDs.get(state);
		while (!totalResultPartitionIDS.isEmpty() && (resultPartitionIDS.size() < cnt)) {
			// Treat deque as a stack.
			resultPartitionIDS.add(totalResultPartitionIDS.pollLast());
		}
		assertEquals(cnt, resultPartitionIDS.size());
		return resultPartitionIDS;
	}

	private void changeResultPartitionState(
		ResultPartitionState previousState,
		ResultPartitionState expectedState,
		int cnt) {

		changeResultPartitionState(previousState, expectedState, cnt, new Configuration());
	}

	private void changeResultPartitionState(
		ResultPartitionState previousState,
		ResultPartitionState expectedState,
		int cnt,
		Configuration configuration) {

		// Validate state transition.
		assertTrue(previousState.ordinal() < expectedState.ordinal());
		assertTrue(!previousState.equals(ResultPartitionState.CONSUMED));

		// Simple state machine, achieve state transition step by step.
		ResultPartitionState currState = previousState;
		while (!expectedState.equals(currState)) {
			switch (currState) {
				case UNDEFINED:
					transitResultPartitionFromUndefinedToUnfinishedNoConfig(cnt);
					break;
				case UNFINISHED_NO_CONFIG:
					transitResultPartitionWriteConfig(cnt, configuration.getBoolean(ENABLE_CUSTOMIZED_TTL));
					break;
				case UNFINISHED_HAS_CONFIG:
					transitResultPartitionFromUnfinishedToUnconsumed(cnt);
					break;
				case UNCONSUMED:
					transitResultPartitionFromUnconsumedToConsumed(cnt);
					break;
				default:
					assertTrue("Unreachable branch", false);
			}
			currState = ResultPartitionState.values()[currState.ordinal() + 1];
		}
	}

	private void transitResultPartitionFromUndefinedToUnfinishedNoConfig(int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(ResultPartitionState.UNDEFINED, cnt);
		String[] rootDirs = externalBlockShuffleServiceConfiguration.getDirToDiskType().keySet().toArray(
			new String[externalBlockShuffleServiceConfiguration.getDirToDiskType().size()]);
		Random random = new Random();
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Transit from UNDEFINED to UNFINISHED_NO_CONFIG: " + resultPartitionID);
			String selectedRootDir = rootDirs[Math.abs(random.nextInt()) % rootDirs.length];
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			String user = appIdToUser.get(appId);
			Path resultPartitionDir = new Path(ExternalBlockShuffleUtils.generatePartitionRootPath(
				selectedRootDir + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId),
				resultPartitionID.getProducerId().toString(),
				resultPartitionID.getPartitionId().toString()));
			resultPartitionIDToLocalDir.put(resultPartitionID,
				new Tuple2<>(selectedRootDir, resultPartitionDir.toString() + "/"));
			// Treat deque as a stack.
			stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_NO_CONFIG).offerLast(resultPartitionID);
			try {
				FILE_SYSTEM.mkdirs(resultPartitionDir);
				assertTrue("ResultPartition's directory should exist.", FILE_SYSTEM.exists(resultPartitionDir));
			} catch (Exception e) {
				assertTrue("Fail to generate result partition dir " + selectedRootDir + ", exception: " + e.getMessage(), false);
			}
		});
	}

	private void transitResultPartitionWriteConfig(int cnt, boolean enableCustomizedTTL) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(ResultPartitionState.UNFINISHED_NO_CONFIG, cnt);

		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Write customized config for: " + resultPartitionID);
			// Treat deque as a stack.
			stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_HAS_CONFIG).offerLast(resultPartitionID);

			if (enableCustomizedTTL) {
				String appId = resultPartitionIDToAppId.get(resultPartitionID);
				String user = appIdToUser.get(appId);
				Configuration taskManagerConfig = new Configuration();
				taskManagerConfig.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS,
					resultPartitionIDToLocalDir.get(resultPartitionID).f0 + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId));

				taskManagerConfig.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_UNCONSUMED_PARTITION_TTL_IN_SECONDS,
					(int) (CUSTOMIZED_UNCONSUMED_PARTITION_TTL / 1000));
				taskManagerConfig.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS,
					(int) (CUSTOMIZED_PARTIAL_CONSUMED_PARTITION_TTL / 1000));
				taskManagerConfig.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_CONSUMED_PARTITION_TTL_IN_SECONDS,
					(int) (CUSTOMIZED_CONSUMED_PARTITION_TTL / 1000));
				taskManagerConfig.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_UNFINISHED_PARTITION_TTL_IN_SECONDS,
					(int) (CUSTOMIZED_UNFINISHED_PARTITION_TTL / 1000));

				ExternalResultPartition<Integer> externalResultPartition = spy(new ExternalResultPartition<>(
					taskManagerConfig,
					"",
					new JobID(),
					resultPartitionID,
					ResultPartitionType.BLOCKING,
					10,
					10,
					mock(MemoryManager.class),
					mock(IOManager.class)));

				assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).f1, externalResultPartition.getPartitionRootPath());
				try {
					externalResultPartition.writeConfigFile(FILE_SYSTEM);
				} catch (Exception e) {
					fail("Fail to write result partition config file");
				}
			}
		});
	}

	private void transitResultPartitionFromUnfinishedToUnconsumed(int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(ResultPartitionState.UNFINISHED_HAS_CONFIG, cnt);
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Transit from UNFINISHED_HAS_CONFIG to UNCONSUMED: " + resultPartitionID);
			// Treat deque as a stack.
			stateToResultPartitionIDs.get(ResultPartitionState.UNCONSUMED).offerLast(resultPartitionID);
			String finishFile = ExternalBlockShuffleUtils.generateFinishedPath(
				resultPartitionIDToLocalDir.get(resultPartitionID).f1);
			try {
				FILE_SYSTEM.create(new Path(finishFile), FileSystem.WriteMode.OVERWRITE);
				assertTrue("Fail to create finish file: " + finishFile, FILE_SYSTEM.exists(new Path(finishFile)));
			} catch (IOException e) {
				assertTrue("Caught exception when creating finish file " + finishFile, false);
			}
		});
	}

	private void transitResultPartitionFromUnconsumedToConsumed(int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(ResultPartitionState.UNCONSUMED, cnt);
		resultPartitionIDS.forEach(resultPartitionID -> {
			// Treat deque as a stack.
			stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).offerLast(resultPartitionID);
			LOG.debug("Transit from UNCONSUMED to CONSUMED: " + resultPartitionID);
			try {
				LocalResultPartitionResolver.ResultPartitionFileInfo descriptor = resultPartitionResolver.getResultPartitionDir(
					resultPartitionID);
				assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).f0, descriptor.getRootDir());
				assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).f1, descriptor.getPartitionDir());
			} catch (IOException e) {
				assertTrue("Caught exception when getResultPartitionDir, exception: " + e.getMessage(), false);
			}
		});
	}

	private void removeResultPartitionByState(ResultPartitionState state, int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(state, cnt);
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Transit from " + state + " to REMOVED: " + resultPartitionID);
			removedResultPartitionIDs.add(resultPartitionID);
			try {
				Tuple2<String, String> rootDirAndPartitionDir = resultPartitionIDToLocalDir.get(resultPartitionID);
				FILE_SYSTEM.delete(new Path(rootDirAndPartitionDir.f1), true);
				assertTrue("Fail to delete result partition dir " + rootDirAndPartitionDir.f1,
					!FILE_SYSTEM.exists(new Path(rootDirAndPartitionDir.f1)));
			} catch (IOException e) {
				// Do nothing.
			}
		});
	}

	private void removeResultPartitionById(Set<ResultPartitionID> resultPartitionIDS) {
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Transit from ANY to REMOVED: " + resultPartitionID);
			// Refresh result partition state map.
			stateToResultPartitionIDs.forEach((state, resultPartitionIDsPerState) -> {
				resultPartitionIDsPerState.remove(resultPartitionID);
			});
			removedResultPartitionIDs.add(resultPartitionID);
			try {
				Tuple2<String, String> rootDirAndPartitionDir = resultPartitionIDToLocalDir.get(resultPartitionID);
				FILE_SYSTEM.delete(new Path(rootDirAndPartitionDir.f1), true);
				assertTrue("Fail to delete result partition dir " + rootDirAndPartitionDir.f1,
					!FILE_SYSTEM.exists(new Path(rootDirAndPartitionDir.f1)));
			} catch (IOException e) {
				// Do nothing.
			}
		});
	}

	private void triggerRecycleConsumedResultPartition(int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = pollResultPartitionIDS(ResultPartitionState.CONSUMED, cnt);
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Transit from CONSUMED to RECYCLED(REMOVED): " + resultPartitionID);
			toBeRemovedResultPartitionIDs.add(resultPartitionID);
			resultPartitionResolver.recycleResultPartition(resultPartitionID);
		});
	}

	private void validateResultPartitionMetaByState() {
		validateResultPartitionMetaByState(new Configuration());
	}

	private void validateResultPartitionMetaByState(Configuration configuration) {
		stateToResultPartitionIDs.get(ResultPartitionState.UNDEFINED).forEach(resultPartitionID -> {
			assertTrue("Undefined ResultPartition should not be in the resolver.",
				!resultPartitionResolver.resultPartitionMap.contains(resultPartitionID));
		});

		stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_NO_CONFIG).forEach(resultPartitionID -> {
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			YarnLocalResultPartitionResolver.YarnResultPartitionFileInfo fileInfo =
				resultPartitionResolver.resultPartitionMap.get(resultPartitionID);

			assertTrue("Resolver should find the directory for " + resultPartitionID, fileInfo != null);
			assertTrue(resultPartitionID.toString(), !fileInfo.isReadyToBeConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.isConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.needToDelete());
			assertTrue(resultPartitionID.toString(), !fileInfo.isConfigLoaded());
			assertEquals(UNCONSUMED_PARTITION_TTL, fileInfo.getUnconsumedPartitionTTL());
			assertEquals(UNFINISHED_PARTITION_TTL, fileInfo.getUnfinishedPartitionTTL());
			assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() > 0L);
			assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() == -1L);
			assertEquals(appId, fileInfo.getAppId());
			assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID), fileInfo.getRootDirAndPartitionDir());
		});

		stateToResultPartitionIDs.get(ResultPartitionState.UNFINISHED_HAS_CONFIG).forEach(resultPartitionID -> {
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			YarnLocalResultPartitionResolver.YarnResultPartitionFileInfo fileInfo =
				resultPartitionResolver.resultPartitionMap.get(resultPartitionID);

			assertTrue("Resolver should find the directory for " + resultPartitionID, fileInfo != null);
			assertTrue(resultPartitionID.toString(), !fileInfo.isReadyToBeConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.isConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.needToDelete());

			if (configuration.getBoolean(ENABLE_CUSTOMIZED_TTL)) {
				assertTrue(resultPartitionID.toString(), fileInfo.isConfigLoaded());
				assertEquals(CUSTOMIZED_UNCONSUMED_PARTITION_TTL, fileInfo.getUnconsumedPartitionTTL());
				assertEquals(CUSTOMIZED_PARTIAL_CONSUMED_PARTITION_TTL, fileInfo.getPartialConsumedPartitionTTL());
				assertEquals(CUSTOMIZED_CONSUMED_PARTITION_TTL, fileInfo.getConsumedPartitionTTL());
				assertEquals(CUSTOMIZED_UNFINISHED_PARTITION_TTL, fileInfo.getUnfinishedPartitionTTL());
			}

			assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() > 0L);
			assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() == -1L);
			assertEquals(appId, fileInfo.getAppId());
			assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID), fileInfo.getRootDirAndPartitionDir());
		});

		stateToResultPartitionIDs.get(ResultPartitionState.UNCONSUMED).forEach(resultPartitionID -> {
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			YarnLocalResultPartitionResolver.YarnResultPartitionFileInfo fileInfo =
				resultPartitionResolver.resultPartitionMap.get(resultPartitionID);

			assertTrue("Resolver should find the directory for " + resultPartitionID, fileInfo != null);
			assertTrue(resultPartitionID.toString(), fileInfo.isReadyToBeConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.isConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.needToDelete());
			assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() > 0L);
			assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() > 0L);
			assertEquals(appId, fileInfo.getAppId());
			assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID), fileInfo.getRootDirAndPartitionDir());
		});

		stateToResultPartitionIDs.get(ResultPartitionState.CONSUMED).forEach(resultPartitionID -> {
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			YarnLocalResultPartitionResolver.YarnResultPartitionFileInfo fileInfo =
				resultPartitionResolver.resultPartitionMap.get(resultPartitionID);

			assertTrue("Resolver should find the directory for " + resultPartitionID, fileInfo != null);
			assertTrue(resultPartitionID.toString(), fileInfo.isReadyToBeConsumed());
			assertTrue(resultPartitionID.toString(), fileInfo.isConsumed());
			assertTrue(resultPartitionID.toString(), !fileInfo.needToDelete());
			assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() > 0L);
			assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() > 0L);
			assertEquals(appId, fileInfo.getAppId());
			assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID), fileInfo.getRootDirAndPartitionDir());
		});

		toBeRemovedResultPartitionIDs.forEach(resultPartitionID -> {
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			YarnLocalResultPartitionResolver.YarnResultPartitionFileInfo fileInfo =
				resultPartitionResolver.resultPartitionMap.get(resultPartitionID);

			assertTrue("Resolver should find the directory for " + resultPartitionID, fileInfo != null);
			assertTrue(resultPartitionID.toString(), fileInfo.isReadyToBeConsumed());
			assertTrue(resultPartitionID.toString(), fileInfo.isConsumed());
			assertTrue(resultPartitionID.toString(), fileInfo.needToDelete());
			assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() == -1L);
			assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() > 0L);
			assertEquals(appId, fileInfo.getAppId());
			assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID), fileInfo.getRootDirAndPartitionDir());
		});

		removedResultPartitionIDs.forEach(resultPartitionID -> {
			assertTrue("Removed ResultPartition should not be in the resolver.",
				!resultPartitionResolver.resultPartitionMap.contains(resultPartitionID));
			Tuple2<String, String> rootDirAndPartitionDir = resultPartitionIDToLocalDir.get(resultPartitionID);
			try {
				boolean exist = FILE_SYSTEM.exists(new Path(rootDirAndPartitionDir.f1));
				assertTrue("ResultPartition directory should be recycled: " + rootDirAndPartitionDir.f1, !exist);
			} catch (IOException e) {
				// Do nothing.
			}
		});
	}

	private Map<String, Set<ResultPartitionID>> aggregateRemainingResultPartitionByAppId() {
		Map<String, Set<ResultPartitionID>> appIdToResultPartitionIDs = new HashMap<>();
		appIdToUser.forEach((appId, user) -> {
			appIdToResultPartitionIDs.put(appId, new HashSet<>());
		});
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			if (state != ResultPartitionState.UNDEFINED) {
				resultPartitionIDs.forEach(resultPartitionID -> {
					appIdToResultPartitionIDs.get(resultPartitionIDToAppId.get(resultPartitionID))
						.add(resultPartitionID);
				});
			}
		});
		return appIdToResultPartitionIDs;
	}
}
