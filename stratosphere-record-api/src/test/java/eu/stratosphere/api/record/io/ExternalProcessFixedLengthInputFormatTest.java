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

package eu.stratosphere.api.record.io;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.io.statistics.BaseStatistics;
import eu.stratosphere.api.record.io.ExternalProcessFixedLengthInputFormat;
import eu.stratosphere.api.record.io.ExternalProcessInputFormat;
import eu.stratosphere.api.record.io.ExternalProcessInputSplit;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.OperatingSystem;

public class ExternalProcessFixedLengthInputFormatTest {

private ExternalProcessFixedLengthInputFormat<ExternalProcessInputSplit> format;
	
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
		
		if(OperatingSystem.isWindows())
			return;
		
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
			format.waitForProcessToFinish();
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
		config.setString(ExternalProcessInputFormat.ALLOWEDEXITCODES_PARAMETER_KEY,"0,1,2");
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
		PactRecord record = new PactRecord();
	    	    
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
		PactRecord record = new PactRecord();

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
		PactRecord record = new PactRecord();

		boolean incompleteRecordDetected = false;
		@SuppressWarnings("unused")
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
	
	private final class MyExternalProcessTestInputFormat extends ExternalProcessFixedLengthInputFormat<ExternalProcessInputSplit> {
		private static final long serialVersionUID = 1L;

		public static final String FAILCOUNT_PARAMETER_KEY = "test.failingCount";
		
		private long cnt = 0;
		private int failCnt;
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			failCnt = parameters.getInteger(FAILCOUNT_PARAMETER_KEY, Integer.MAX_VALUE);
		}
		
		@Override
		public boolean readBytes(PactRecord record, byte[] bytes, int startPos) {

			if(cnt == failCnt) {
				throw new RuntimeException("This is a test exception!");
			}
			
			int v1 = 0;
			v1 = v1        | (0xFF & bytes[startPos+0]);
			v1 = (v1 << 8) | (0xFF & bytes[startPos+1]);
			v1 = (v1 << 8) | (0xFF & bytes[startPos+2]);
			v1 = (v1 << 8) | (0xFF & bytes[startPos+3]);
			
			int v2 = 0;
			v2 = v2        | (0xFF & bytes[startPos+4]);
			v2 = (v2 << 8) | (0xFF & bytes[startPos+5]);
			v2 = (v2 << 8) | (0xFF & bytes[startPos+6]);
			v2 = (v2 << 8) | (0xFF & bytes[startPos+7]);
			
			record.setField(0,new PactInteger(v1));
			record.setField(1,new PactInteger(v2));
			
			cnt++;
			
			return true;
		}

		@Override
		public ExternalProcessInputSplit[] createInputSplits(int minNumSplits)
				throws IOException {
			return null;
		}

		@Override
		public Class<GenericInputSplit> getInputSplitType() {
			return GenericInputSplit.class;
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
			return null;
		}

	}
}
