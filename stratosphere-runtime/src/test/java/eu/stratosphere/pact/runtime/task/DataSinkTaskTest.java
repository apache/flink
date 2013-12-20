/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

import eu.stratosphere.api.java.record.io.DelimitedOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparatorFactory;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.InfiniteInputIterator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;
import eu.stratosphere.pact.runtime.test.util.UniformRecordGenerator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;

public class DataSinkTaskTest extends TaskTestBase
{
	private static final Log LOG = LogFactory.getLog(DataSinkTaskTest.class);
	
	private final String tempTestPath = Path.constructTestPath("dst_test");
	
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
		
		super.initEnvironment(1024 * 1024);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
		
		DataSinkTask<Record> testTask = new DataSinkTask<Record>();

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
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
		
		super.initEnvironment(1024 * 1024);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, 0, 0, false), 0);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt, 0, false), 0);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt*2, 0, false), 0);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, keyCnt*3, 0, false), 0);
		
		DataSinkTask<Record> testTask = new DataSinkTask<Record>();

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
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
		
		super.initEnvironment(1024 * 1024 * 4);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);
		
		DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		
		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1},((Class<? extends Key>[])new Class[]{IntValue.class})), 
				0);
		super.getTaskConfig().setMemoryInput(0, 4 * 1024 * 1024);
		super.getTaskConfig().setFilehandlesInput(0, 8);
		super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

		super.registerFileOutputTask(testTask, MockOutputFormat.class, new File(tempTestPath).toURI().toString());
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
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
		
		super.initEnvironment(1024 * 1024);
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
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
		
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testFailingSortingDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		super.initEnvironment(4 * 1024 * 1024);
		super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);

		DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);
		
		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1},((Class<? extends Key>[])new Class[]{IntValue.class})), 
				0);
		super.getTaskConfig().setMemoryInput(0, 4 * 1024 * 1024);
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
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
		
	}
	
	@Test
	public void testCancelDataSinkTask() {
		
		super.initEnvironment(1024 * 1024);
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
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
				
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testCancelSortingDataSinkTask() {
		
		super.initEnvironment(4 * 1024 * 1024);
		super.addInput(new InfiniteInputIterator(), 0);
		
		final DataSinkTask<Record> testTask = new DataSinkTask<Record>();
		Configuration stubParams = new Configuration();
		super.getTaskConfig().setStubParameters(stubParams);
		
		// set sorting
		super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
		super.getTaskConfig().setInputComparator(
				new RecordComparatorFactory(new int[]{1},((Class<? extends Key>[])new Class[]{IntValue.class})), 
				0);
		super.getTaskConfig().setMemoryInput(0, 4 * 1024 * 1024);
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
	
}

