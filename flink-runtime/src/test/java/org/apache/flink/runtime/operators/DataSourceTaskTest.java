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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;

import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.testutils.NirvanaOutputList;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Test;

public class DataSourceTaskTest extends TaskTestBase {

	private static final int MEMORY_MANAGER_SIZE = 1024 * 1024;

	private static final int NETWORK_BUFFER_SIZE = 1024;

	private List<Record> outList;
	
	private String tempTestPath = Path.constructTestPath(DataSourceTaskTest.class, "dst_test");
	
	@After
	public void cleanUp() {
		File tempTestFile = new File(this.tempTestPath);
		if(tempTestFile.exists()) {
			tempTestFile.delete();
		}
	}
	
	@Test
	public void testDataSourceTask() {
		int keyCnt = 100;
		int valCnt = 20;
		
		this.outList = new ArrayList<Record>();
		
		try {
			InputFilePreparator.prepareInputFile(new UniformRecordGenerator(keyCnt, valCnt, false), 
				this.tempTestPath, true);
		} catch (IOException e1) {
			Assert.fail("Unable to set-up test input file");
		}
		
		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addOutput(this.outList);
		
		DataSourceTask<Record> testTask = new DataSourceTask<Record>();
		
		super.registerFileInputTask(testTask, MockInputFormat.class, new File(tempTestPath).toURI().toString(), "\n");
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			System.err.println(e);
			Assert.fail("Invoke method caused exception.");
		}
		
		Assert.assertTrue("Invalid output size. Expected: "+(keyCnt*valCnt)+" Actual: "+this.outList.size(),
			this.outList.size() == keyCnt * valCnt);
		
		HashMap<Integer,HashSet<Integer>> keyValueCountMap = new HashMap<Integer, HashSet<Integer>>(keyCnt);
		
		for (Record kvp : this.outList) {
			
			int key = kvp.getField(0, IntValue.class).getValue();
			int val = kvp.getField(1, IntValue.class).getValue();
			
			if(!keyValueCountMap.containsKey(key)) {
				keyValueCountMap.put(key,new HashSet<Integer>());
			}
			keyValueCountMap.get(key).add(val);
			
		}
		
		Assert.assertTrue("Invalid key count in out file. Expected: "+keyCnt+" Actual: "+keyValueCountMap.keySet().size(),
			keyValueCountMap.keySet().size() == keyCnt);
		
		for(Integer mapKey : keyValueCountMap.keySet()) {
			Assert.assertTrue("Invalid value count for key: "+mapKey+". Expected: "+valCnt+" Actual: "+keyValueCountMap.get(mapKey).size(),
				keyValueCountMap.get(mapKey).size() == valCnt);
		}
		
	}
	
	@Test
	public void testFailingDataSourceTask() {
		int keyCnt = 20;
		int valCnt = 10;
		
		this.outList = new NirvanaOutputList();
		
		try {
			InputFilePreparator.prepareInputFile(new UniformRecordGenerator(keyCnt, valCnt, false), 
				this.tempTestPath, false);
		} catch (IOException e1) {
			Assert.fail("Unable to set-up test input file");
		}

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addOutput(this.outList);
		
		DataSourceTask<Record> testTask = new DataSourceTask<Record>();

		super.registerFileInputTask(testTask, MockFailingInputFormat.class, new File(tempTestPath).toURI().toString(), "\n");
		
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
	public void testCancelDataSourceTask() {
		int keyCnt = 20;
		int valCnt = 4;

		super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
		super.addOutput(new NirvanaOutputList());
		
		try {
			InputFilePreparator.prepareInputFile(new UniformRecordGenerator(keyCnt, valCnt, false), 
				this.tempTestPath, false);
		} catch (IOException e1) {
			Assert.fail("Unable to set-up test input file");
		}
		
		final DataSourceTask<Record> testTask = new DataSourceTask<Record>();

		super.registerFileInputTask(testTask, MockDelayingInputFormat.class,  new File(tempTestPath).toURI().toString(), "\n");
		
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

	
	private static class InputFilePreparator {
		public static void prepareInputFile(MutableObjectIterator<Record> inIt, String inputFilePath, boolean insertInvalidData)
		throws IOException
		{
			FileWriter fw = new FileWriter(inputFilePath);
			BufferedWriter bw = new BufferedWriter(fw);
			
			if (insertInvalidData) {
				bw.write("####_I_AM_INVALID_########\n");
			}
			
			Record rec = new Record();
			while ((rec = inIt.next(rec)) != null) {
				IntValue key = rec.getField(0, IntValue.class);
				IntValue value = rec.getField(1, IntValue.class);
				
				bw.write(key.getValue() + "_" + value.getValue() + "\n");
			}
			if (insertInvalidData) {
				bw.write("####_I_AM_INVALID_########\n");
			}
			
			bw.flush();
			bw.close();
		}
	}
	
	public static class MockInputFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		
		@Override
		public Record readRecord(Record target, byte[] record, int offset, int numBytes) {
			
			String line = new String(record, offset, numBytes);
			
			try {
				this.key.setValue(Integer.parseInt(line.substring(0,line.indexOf("_"))));
				this.value.setValue(Integer.parseInt(line.substring(line.indexOf("_")+1,line.length())));
			}
			catch(RuntimeException re) {
				return null;
			}
			
			target.setField(0, this.key);
			target.setField(1, this.value);
			return target;
		}
	}
	
	public static class MockDelayingInputFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		
		@Override
		public Record readRecord(Record target, byte[] record, int offset, int numBytes) {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				return null;
			}
			
			String line = new String(record, offset, numBytes);
			
			try {
				this.key.setValue(Integer.parseInt(line.substring(0,line.indexOf("_"))));
				this.value.setValue(Integer.parseInt(line.substring(line.indexOf("_")+1,line.length())));
			}
			catch(RuntimeException re) {
				return null;
			}
			
			target.setField(0, this.key);
			target.setField(1, this.value);
			return target;
		}
		
	}
	
	public static class MockFailingInputFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final IntValue key = new IntValue();
		private final IntValue value = new IntValue();
		
		private int cnt = 0;
		
		@Override
		public Record readRecord(Record target, byte[] record, int offset, int numBytes) {
			
			if(this.cnt == 10) {
				throw new RuntimeException("Excpected Test Exception.");
			}
			
			this.cnt++;
			
			String line = new String(record, offset, numBytes);
			
			try {
				this.key.setValue(Integer.parseInt(line.substring(0,line.indexOf("_"))));
				this.value.setValue(Integer.parseInt(line.substring(line.indexOf("_")+1,line.length())));
			}
			catch(RuntimeException re) {
				return null;
			}
			
			target.setField(0, this.key);
			target.setField(1, this.value);
			return target;
		}
	}
}
