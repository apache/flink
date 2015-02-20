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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.typeutils.record.RecordComparatorFactory;
import org.apache.flink.api.java.record.io.DelimitedOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.reader.IteratorWrappingMockSingleInputGate;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.operators.testutils.InfiniteInputIterator;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Task.class, BufferWriter.class})
public class DataSinkTaskTest extends TaskTestBase
{
	private static final Logger LOG = LoggerFactory.getLogger(DataSinkTaskTest.class);

	private static final int MEMORY_MANAGER_SIZE = 3 * 1024 * 1024;

	private static final int NETWORK_BUFFER_SIZE = 1024;

	private final String tempTestPath = constructTestPath(DataSinkTaskTest.class, "dst_test");

	@After
	public void cleanUp() {
		File tempTestFile = new File(this.tempTestPath);
		if(tempTestFile.exists()) {
			tempTestFile.delete();
		}
	}

	@Test
	public void testDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());

		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug("Exception while invoking the test task.", e);
			Assert.fail("Invoke method caused exception.");
		}

		File tempTestFile = new File(this.tempTestPath);

		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());

		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(tempTestFile);
			br = new BufferedReader(fr);

			HashMap<Integer,HashSet<Integer>> keyValueCountMap = new HashMap<Integer, HashSet<Integer>>(keyCnt);

			while(br.ready()) {
				String line = br.readLine();

				Integer key = Integer.parseInt(line.substring(0,line.indexOf("_")));
				Integer val = Integer.parseInt(line.substring(line.indexOf("_")+1,line.length()));

				if(!keyValueCountMap.containsKey(key)) {
					keyValueCountMap.put(key,new HashSet<Integer>());
				}
				keyValueCountMap.get(key).add(val);
			}

			Assert.assertTrue("Invalid key count in out file. Expected: "+keyCnt+" Actual: "+keyValueCountMap.keySet().size(),
				keyValueCountMap.keySet().size() == keyCnt);

			for(Integer key : keyValueCountMap.keySet()) {
				Assert.assertTrue("Invalid value count for key: "+key+". Expected: "+valCnt+" Actual: "+keyValueCountMap.get(key).size(),
					keyValueCountMap.get(key).size() == valCnt);
			}

		} catch (FileNotFoundException e) {
			Assert.fail("Out file got lost...");
		} catch (IOException ioe) {
			Assert.fail("Caught IOE while reading out file");
		} finally {
			if (br != null) {
				try { br.close(); } catch (Throwable t) {}
			}
			if (fr != null) {
				try { fr.close(); } catch (Throwable t) {}
			}
		}
	}

	@Test
	public void testUnionDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

		IteratorWrappingMockSingleInputGate<?>[] readers = new IteratorWrappingMockSingleInputGate[4];
		readers[0] = super.addInput(new UniformRecordGenerator(keyCnt, valCnt, 0, 0, false), 0, false);
		readers[1] = super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt, 0, false), 0, false);
		readers[2] = super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt * 2, 0, false), 0, false);
		readers[3] = super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt * 3, 0, false), 0, false);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());

		try {
			// For the union reader to work, we need to start notifications *after* the union reader
			// has been initialized.
			for (IteratorWrappingMockSingleInputGate<?> reader : readers) {
				reader.read();
			}

			testTask.invoke();
		} catch (Exception e) {
			LOG.debug("Exception while invoking the test task.", e);
			Assert.fail("Invoke method caused exception.");
		}

		File tempTestFile = new File(this.tempTestPath);

		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());

		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(tempTestFile);
			br = new BufferedReader(fr);

			HashMap<Integer,HashSet<Integer>> keyValueCountMap = new HashMap<Integer, HashSet<Integer>>(keyCnt);

			while(br.ready()) {
				String line = br.readLine();

				Integer key = Integer.parseInt(line.substring(0,line.indexOf("_")));
				Integer val = Integer.parseInt(line.substring(line.indexOf("_")+1,line.length()));

				if(!keyValueCountMap.containsKey(key)) {
					keyValueCountMap.put(key,new HashSet<Integer>());
				}
				keyValueCountMap.get(key).add(val);
			}

			Assert.assertTrue("Invalid key count in out file. Expected: "+keyCnt+" Actual: "+keyValueCountMap.keySet().size(),
				keyValueCountMap.keySet().size() == keyCnt * 4);

			for(Integer key : keyValueCountMap.keySet()) {
				Assert.assertTrue("Invalid value count for key: "+key+". Expected: "+valCnt+" Actual: "+keyValueCountMap.get(key).size(),
					keyValueCountMap.get(key).size() == valCnt);
			}

		} catch (FileNotFoundException e) {
			Assert.fail("Out file got lost...");
		} catch (IOException ioe) {
			Assert.fail("Caught IOE while reading out file");
		} finally {
			if (br != null) {
				try { br.close(); } catch (Throwable t) {}
			}
			if (fr != null) {
				try { fr.close(); } catch (Throwable t) {}
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSortingDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;
		double memoryFraction = 1.0;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();

		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1},((Class<? extends Key<?>>[])new Class[]{IntValue.class})),
				0);
		super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
		super.getTaskConfig().setFilehandlesInput(0, 8);
		super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());

		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug("Exception while invoking the test task.", e);
			Assert.fail("Invoke method caused exception.");
		}

		File tempTestFile = new File(this.tempTestPath);

		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());

		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(tempTestFile);
			br = new BufferedReader(fr);

			Set<Integer> keys = new HashSet<Integer>();

			int curVal = -1;
			while(br.ready()) {
				String line = br.readLine();

				Integer key = Integer.parseInt(line.substring(0,line.indexOf("_")));
				Integer val = Integer.parseInt(line.substring(line.indexOf("_")+1,line.length()));

				// check that values are in correct order
				Assert.assertTrue("Values not in ascending order", val >= curVal);
				// next value hit
				if(val > curVal) {
					if(curVal != -1) {
						// check that we saw 100 distinct keys for this values
						Assert.assertTrue("Keys missing for value", keys.size() == 100);
					}
					// empty keys set
					keys.clear();
					// update current value
					curVal = val;
				}

				Assert.assertTrue("Duplicate key for value", keys.add(key));
			}

		} catch (FileNotFoundException e) {
			Assert.fail("Out file got lost...");
		} catch (IOException ioe) {
			Assert.fail("Caught IOE while reading out file");
		} finally {
			if (br != null) {
				try { br.close(); } catch (Throwable t) {}
			}
			if (fr != null) {
				try { fr.close(); } catch (Throwable t) {}
			}
		}
	}

	@Test
	public void testFailingDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);

		super.registerFileOutputTask(testTask, MockFailingOutputFormat.class, new File(tempTestPath).toURI().toString());

		boolean stubFailed = false;

		try {
			testTask.invoke();
		} catch (Exception e) {
			stubFailed = true;
		}
		Assert.assertTrue("Function exception was not forwarded.", stubFailed);

		// assert that temp file was created
		File tempTestFile = new File(this.tempTestPath);
		Assert.assertFalse("Temp output file has not been removed", tempTestFile.exists());

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFailingSortingDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;;
		double memoryFraction = 1.0;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);

		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1}, ((Class<? extends Key<?>>[]) new Class[]{IntValue.class})),
				0);
		super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
		super.getTaskConfig().setFilehandlesInput(0, 8);
		super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

		super.registerFileOutputTask(testTask, MockFailingOutputFormat.class, new File(tempTestPath).toURI().toString());

		boolean stubFailed = false;

		try {
			testTask.invoke();
		} catch (Exception e) {
			stubFailed = true;
		}
		Assert.assertTrue("Function exception was not forwarded.", stubFailed);

		// assert that temp file was created
		File tempTestFile = new File(this.tempTestPath);
		Assert.assertFalse("Temp output file has not been removed", tempTestFile.exists());

	}

	@Test
	public void testCancelDataSinkTask() {

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addInput(new InfiniteInputIterator(), 0);

		final DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);

		super.registerFileOutputTask(testTask, MockOutputFormat.class,  new File(tempTestPath).toURI().toString());

		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testTask.invoke();
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}
			}
		};
		taskRunner.start();

		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, testTask);
		tct.start();

		try {
			tct.join();
			taskRunner.join();
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}

		// assert that temp file was created
		File tempTestFile = new File(this.tempTestPath);
		Assert.assertFalse("Temp output file has not been removed", tempTestFile.exists());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCancelSortingDataSinkTask() {
		double memoryFraction = 1.0;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addInput(new InfiniteInputIterator(), 0);

		final DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);

		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1},((Class<? extends Key<?>>[])new Class[]{IntValue.class})),
				0);
		super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
		super.getTaskConfig().setFilehandlesInput(0, 8);
		super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

		super.registerFileOutputTask(testTask, MockOutputFormat.class,  new File(tempTestPath).toURI().toString());

		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testTask.invoke();
				} catch (Exception ie) {
					ie.printStackTrace();
					Assert.fail("Task threw exception although it was properly canceled");
				}
			}
		};
		taskRunner.start();

		TaskCancelThread tct = new TaskCancelThread(2, taskRunner, testTask);
		tct.start();

		try {
			tct.join();
			taskRunner.join();
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}

	}

	public static class MockOutputFormat extends DelimitedOutputFormat {
		private static final long serialVersionUID = 1L;

		final StringBuilder bld = new StringBuilder();

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
		}

		@Override
		public int serializeRecord(Record rec, byte[] target) throws Exception
		{
			IntValue key = rec.getField(0, IntValue.class);
			IntValue value = rec.getField(1, IntValue.class);

			this.bld.setLength(0);
			this.bld.append(key.getValue());
			this.bld.append('_');
			this.bld.append(value.getValue());

			byte[] bytes = this.bld.toString().getBytes();
			if (bytes.length <= target.length) {
				System.arraycopy(bytes, 0, target, 0, bytes.length);
				return bytes.length;
			}
			// else
			return -bytes.length;
		}

	}

	public static class MockFailingOutputFormat extends MockOutputFormat {
		private static final long serialVersionUID = 1L;

		int cnt = 0;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
		}

		@Override
		public int serializeRecord(Record rec, byte[] target) throws Exception
		{
			if (++this.cnt >= 10) {
				throw new RuntimeException("Expected Test Exception");
			}
			return super.serializeRecord(rec, target);
		}
	}
	
	public static String constructTestPath(Class<?> forClass, String folder) {
		// we create test path that depends on class to prevent name clashes when two tests
		// create temp files with the same name
		String path = System.getProperty("java.io.tmpdir");
		if (!(path.endsWith("/") || path.endsWith("\\")) ) {
			path += System.getProperty("file.separator");
		}
		path += (forClass.getName() + "-" + folder);
		return path;
	}
	
	public static String constructTestURI(Class<?> forClass, String folder) {
		return new File(constructTestPath(forClass, folder)).toURI().toString();
	}
}

