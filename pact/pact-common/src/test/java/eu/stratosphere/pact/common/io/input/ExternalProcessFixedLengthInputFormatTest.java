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

package eu.stratosphere.pact.common.io.input;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.input.ExternalProcessFixedLengthInputFormat;
import eu.stratosphere.pact.common.io.input.ExternalProcessInputFormat;
import eu.stratosphere.pact.common.io.input.ExternalProcessInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ExternalProcessFixedLengthInputFormatTest {

private ExternalProcessFixedLengthInputFormat<ExternalProcessInputSplit, PactInteger, PactInteger> format;
	
	private final String neverEndingCommand = "cat /dev/urandom";
	private final String thousandRecordsCommand = "dd if=/dev/zero bs=8 count=1000";
	private final String incompleteRecordsCommand = "dd if=/dev/zero bs=7 count=2";
	private final String failingCommand = "ls /I/do/not/exist";
	
	@Before
	public void prepare() {
		format = new MyExternalProcessTestInputFormat();
	}
	
	@Test
	public void testOpen() {
		
		Configuration config = new Configuration();
		config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		ExternalProcessInputSplit split = new ExternalProcessInputSplit(1, this.neverEndingCommand);
	    	    
		boolean processDestroyed = false;
		try {
			format.configure(config);
			format.open(split);
			
			String[] cmd = {"/bin/sh","-c","ps aux | grep -v grep | grep \"cat /dev/urandom\" | wc -l"};
			
			byte[] wcOut = new byte[128];
			Process p = Runtime.getRuntime().exec(cmd);
			p.getInputStream().read(wcOut);
			int pCnt = Integer.parseInt(new String(wcOut).trim());
			Assert.assertTrue(pCnt > 0);

			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			if(e.getMessage().equals("External process was destroyed although stream was not fully read.")) {
				processDestroyed = true;
			}
		} finally {
			Assert.assertTrue(processDestroyed);
		}
	}
	
	@Test
	public void testCheckExitCode() {
		
		Configuration config = new Configuration();
		config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		ExternalProcessInputSplit split = new ExternalProcessInputSplit(1, failingCommand);
		
		format.configure(config);
		boolean invalidExitCode = false;
		try {
			format.open(split);
			// wait for process to start...
			Thread.sleep(100);
			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (InterruptedException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			if(e.getMessage().startsWith("External process did not finish with an allowed exit code:")) {
				invalidExitCode = true;	
			}
		}
		Assert.assertTrue(invalidExitCode);
		
		invalidExitCode = false;
		config.setString(ExternalProcessInputFormat.ALLOWEDEXITCODES_PARAMETER_KEY,"0,2");
		format.configure(config);
		try {
			format.open(split);
			// wait for process to start...
			Thread.sleep(100);
			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (InterruptedException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			if(e.getMessage().startsWith("External process did not finish with an allowed exit code:")) {
				invalidExitCode = true;	
			}
		}
		Assert.assertTrue(!invalidExitCode);
		
	}
	
	@Test
	public void testUserCodeTermination() {
		
		Configuration config = new Configuration();
		config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		config.setInteger(MyExternalProcessTestInputFormat.FAILCOUNT_PARAMETER_KEY, 100);
		ExternalProcessInputSplit split = new ExternalProcessInputSplit(1, this.neverEndingCommand);
		KeyValuePair<PactInteger, PactInteger> record = format.createPair(); 
	    	    
		boolean userException = false;
		boolean processDestroyed = false;
		try {
			format.configure(config);
			format.open(split);
			while(!format.reachedEnd()) {
				try {
					format.nextRecord(record);
				} catch(RuntimeException re) {
					userException = true;
					break;
				}
			}
			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			if(e.getMessage().equals("External process was destroyed although stream was not fully read.")) {
				processDestroyed = true;
			}
		} finally {
			Assert.assertTrue(userException && processDestroyed);
		}
	}
	
	@Test
	public void testReadStream() {
		
		Configuration config = new Configuration();
		config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		ExternalProcessInputSplit split = new ExternalProcessInputSplit(1, this.thousandRecordsCommand);
		KeyValuePair<PactInteger, PactInteger> record = format.createPair(); 

		int cnt = 0;
		try {
			format.configure(config);
			format.open(split);
			while(!format.reachedEnd()) {
				if (format.nextRecord(record)) {
					cnt++;
				}
			}
			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			Assert.fail(e.getMessage());
		}
		Assert.assertTrue(cnt == 1000);
	}
	
	@Test
	public void testReadInvalidStream() {
		
		Configuration config = new Configuration();
		config.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 8);
		ExternalProcessInputSplit split = new ExternalProcessInputSplit(1, this.incompleteRecordsCommand);
		KeyValuePair<PactInteger, PactInteger> record = format.createPair(); 

		boolean incompleteRecordDetected = false;
		int cnt = 0;
		try {
			format.configure(config);
			format.open(split);
			while(!format.reachedEnd()) {
				if (format.nextRecord(record)) {
					cnt++;
				}
			}
			format.close();
		} catch (IOException e) {
			Assert.fail();
		} catch (RuntimeException e) {
			if(e.getMessage().equals("External process produced incomplete record")) {
				incompleteRecordDetected = true;
			} else {
				Assert.fail(e.getMessage());
			}
		}
		Assert.assertTrue(incompleteRecordDetected);
	}
	
	private final class MyExternalProcessTestInputFormat extends ExternalProcessFixedLengthInputFormat<ExternalProcessInputSplit, PactInteger, PactInteger> {

		public static final String FAILCOUNT_PARAMETER_KEY = "test.failingCount";
		
		private long cnt = 0;
		private int failCnt;
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			failCnt = parameters.getInteger(FAILCOUNT_PARAMETER_KEY, Integer.MAX_VALUE);
		}
		
		@Override
		public boolean readBytes(KeyValuePair<PactInteger, PactInteger> pair,
				byte[] pairBytes) {

			if(cnt == failCnt) {
				throw new RuntimeException("This is a test exception!");
			}
			
			int key = 0;
			key = key        | (0xFF & pairBytes[0]);
			key = (key << 8) | (0xFF & pairBytes[1]);
			key = (key << 8) | (0xFF & pairBytes[2]);
			key = (key << 8) | (0xFF & pairBytes[3]);
			
			int val = 0;
			val = val        | (0xFF & pairBytes[4]);
			val = (val << 8) | (0xFF & pairBytes[5]);
			val = (val << 8) | (0xFF & pairBytes[6]);
			val = (val << 8) | (0xFF & pairBytes[7]);
			
			pair.getKey().setValue(key);
			pair.getValue().setValue(val);
			
			cnt++;
			
			return true;
		}

		@Override
		public ExternalProcessInputSplit[] createInputSplits(int minNumSplits)
				throws IOException {
			return null;
		}

		@Override
		public Class<ExternalProcessInputSplit> getInputSplitType() {
			return ExternalProcessInputSplit.class;
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
			return null;
		}

		@Override
		public KeyValuePair<PactInteger, PactInteger> createPair() {
			return new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		}
		
	}
	
}
