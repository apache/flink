/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.InfiniteInputIterator;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskCancelThread;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class DataSinkTaskTest extends TaskTestBase {

	private static final Log LOG = LogFactory.getLog(DataSinkTaskTest.class);
	
	String tempTestPath = System.getProperty("java.io.tmpdir")+"/dst_test";
	
	@After
	public void cleanUp() {
		File tempTestFile = new File(tempTestPath);
		if(tempTestFile.exists()) {
			tempTestFile.delete();
		}
	}
	
	@Test
	public void testDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		super.initEnvironment(1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
		
		DataSinkTask testTask = new DataSinkTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NONE);
		super.getConfiguration().setString(DataSinkTask.SORT_ORDER, Order.NONE.name());
		
		super.registerFileOutputTask(testTask, MockOutputFormat.class, "file://"+tempTestPath);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			LOG.debug(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		File tempTestFile = new File(tempTestPath);
		
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
		
		try {
			FileReader fr = new FileReader(tempTestFile);
			BufferedReader br = new BufferedReader(fr);
			
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
		}
		
	}
	
//	@Test
//	public void testLocalSortAscDataSinkTask() {
//
//		int keyCnt = 100;
//		int valCnt = 20;
//		
//		super.initEnvironment(3*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
//		
//		DataSinkTask testTask = new DataSinkTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
//		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getConfiguration().setString(DataSinkTask.SORT_ORDER, Order.ASCENDING.name());
//		
//		super.registerFileOutputTask(testTask, MockOutputFormat.class, "file://"+tempTestPath);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		File tempTestFile = new File(tempTestPath);
//		
//		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
//		
//		try {
//			FileReader fr = new FileReader(tempTestFile);
//			BufferedReader br = new BufferedReader(fr);
//					
//			Integer oldKey = null;
//			
//			while(br.ready()) {
//				String line = br.readLine();
//				
//				Integer key = Integer.parseInt(line.substring(0,line.indexOf("_")));
//				
//				if(oldKey != null) {
//					Assert.assertTrue("Data is not written in correct order", oldKey.intValue() <= key.intValue());
//				}
//				
//				oldKey = key;
//			}
//			
//		} catch (FileNotFoundException e) {
//			Assert.fail("Out file got lost...");
//		} catch (IOException ioe) {
//			Assert.fail("Caught IOE while reading out file");
//		}
//		
//	}
//	
//	@Test
//	public void testLocalSortDescDataSinkTask() {
//
//		int keyCnt = 100;
//		int valCnt = 20;
//		
//		super.initEnvironment(3*1024*1024);
//		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));
//		
//		DataSinkTask testTask = new DataSinkTask();
//		super.getTaskConfig().setLocalStrategy(LocalStrategy.SORT);
//		super.getTaskConfig().setMemorySize(3 * 1024 * 1024);
//		super.getTaskConfig().setNumFilehandles(4);
//		super.getConfiguration().setString(DataSinkTask.SORT_ORDER, Order.DESCENDING.name());
//		
//		super.registerFileOutputTask(testTask, MockOutputFormat.class, "file://"+tempTestPath);
//		
//		try {
//			testTask.invoke();
//		} catch (Exception e) {
//			LOG.debug(e);
//			e.printStackTrace();
//			Assert.fail("Invoke method caused exception.");
//		}
//		
//		File tempTestFile = new File(tempTestPath);
//		
//		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
//		
//		try {
//			FileReader fr = new FileReader(tempTestFile);
//			BufferedReader br = new BufferedReader(fr);
//			
//			Integer oldKey = null;
//			
//			while(br.ready()) {
//				String line = br.readLine();
//				
//				Integer key = Integer.parseInt(line.substring(0,line.indexOf("_")));
//				
//				if(oldKey != null) {
//					Assert.assertTrue("Data is not written in correct order", oldKey.intValue() >= key.intValue());
//				}
//				
//				oldKey = key;
//			}
//			
//		} catch (FileNotFoundException e) {
//			Assert.fail("Out file got lost...");
//		} catch (IOException ioe) {
//			Assert.fail("Caught IOE while reading out file");
//		}
//		
//	}
	
	@Test
	public void testFailingDataSinkTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		super.initEnvironment(1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt, false));

		DataSinkTask testTask = new DataSinkTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NONE);
		Configuration stubParams = new Configuration();
		stubParams.setString(DataSinkTask.SORT_ORDER, Order.NONE.name());
		super.getTaskConfig().setStubParameters(stubParams);
		
		super.registerFileOutputTask(testTask, MockFailingOutputFormat.class, "file://"+tempTestPath);
		
		boolean stubFailed = false;
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			stubFailed = true;
		}
		
		Assert.assertTrue("Stub exception was not forwarded.", stubFailed);
		
		// assert that temp file was created
		File tempTestFile = new File(tempTestPath);
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
		
	}
	
	@Test
	public void testCancelDataSinkTask() {
		
		super.initEnvironment(1);
		super.addInput(new InfiniteInputIterator());
		
		final DataSinkTask testTask = new DataSinkTask();
		super.getTaskConfig().setLocalStrategy(LocalStrategy.NONE);
		Configuration stubParams = new Configuration();
		stubParams.setString(DataSinkTask.SORT_ORDER, Order.NONE.name());
		super.getTaskConfig().setStubParameters(stubParams);
		
		super.registerFileOutputTask(testTask, MockOutputFormat.class,  "file://"+tempTestPath);
		
		Thread taskRunner = new Thread() {
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
		File tempTestFile = new File(tempTestPath);
		Assert.assertTrue("Temp output file does not exist",tempTestFile.exists());
				
	}
	
	public static class MockOutputFormat extends DelimitedOutputFormat
	{
		final StringBuilder bld = new StringBuilder();
		
		@Override
		public int serializeRecord(PactRecord rec, byte[] target) throws Exception
		{
			PactInteger key = rec.getField(0, PactInteger.class);
			PactInteger value = rec.getField(1, PactInteger.class);
		
			bld.setLength(0);
			bld.append(key.getValue());
			bld.append('_');
			bld.append(value.getValue());
			
			byte[] bytes = bld.toString().getBytes();
			if (bytes.length <= target.length) {
				System.arraycopy(bytes, 0, target, 0, bytes.length);
				return bytes.length;
			}
			else {
				return -bytes.length;
			}
		}
		
	}
	
	public static class MockFailingOutputFormat extends MockOutputFormat {

		int cnt = 0;
		
		@Override
		public int serializeRecord(PactRecord rec, byte[] target) throws Exception
		{
			if (++cnt >= 10) {
				throw new RuntimeException("Expected Test Exception");
			}
			return super.serializeRecord(rec, target);
		}
	}
}

