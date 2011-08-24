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
import eu.stratosphere.pact.common.io.input.ExternalProcessInputFormat;
import eu.stratosphere.pact.common.io.input.ExternalProcessInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ExternalProcessInputFormatTest {

	private ExternalProcessInputFormat<ExternalProcessInputSplit, PactInteger, PactInteger> format;
	
	private final String neverEndingCommand = "cat /dev/urandom";
	private final String thousandRecordsCommand = "dd if=/dev/zero bs=8 count=1000";
	private final String failingCommand = "ls /I/do/not/exist";
	
	@Before
	public void prepare() {
		format = new MyExternalProcessTestInputFormat();
	}
	
	@Test
	public void testOpen() {
		
		Configuration config = new Configuration();
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
	
	private final class MyExternalProcessTestInputFormat extends ExternalProcessInputFormat<ExternalProcessInputSplit, PactInteger, PactInteger> {

		public static final String FAILCOUNT_PARAMETER_KEY = "test.failingCount";
		
		private byte[] buf = new byte[8];
		
		private long cnt = 0;
		private int failCnt;
		private boolean end;
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			failCnt = parameters.getInteger(FAILCOUNT_PARAMETER_KEY, Integer.MAX_VALUE);
		}
		
		@Override
		public void open(ExternalProcessInputSplit split) throws IOException {
			super.open(split);
			
			this.end = false;
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
		public boolean nextRecord(KeyValuePair<PactInteger, PactInteger> record) throws IOException {
			
			if(cnt > failCnt) {
				throw new RuntimeException("This is a test exception!");
			}
			
			int totalReadCnt = 0;
			
			do {
				int readCnt = super.extProcOutStream.read(buf, totalReadCnt, buf.length-totalReadCnt);
				
				if(readCnt == -1) {
					this.end = true;
					return false;
				} else {
					totalReadCnt += readCnt;
				}
				
			} while(totalReadCnt != 8);
				
			int key = 0;
			key = key        | (0xFF & buf[0]);
			key = (key << 8) | (0xFF & buf[1]);
			key = (key << 8) | (0xFF & buf[2]);
			key = (key << 8) | (0xFF & buf[3]);
			
			int val = 0;
			val = val        | (0xFF & buf[4]);
			val = (val << 8) | (0xFF & buf[5]);
			val = (val << 8) | (0xFF & buf[6]);
			val = (val << 8) | (0xFF & buf[7]);
			
			record.getKey().setValue(key);
			record.getValue().setValue(val);
			
			this.cnt++;
			
			return true;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return this.end;
		}

		@Override
		public KeyValuePair<PactInteger, PactInteger> createPair() {
			return new KeyValuePair<PactInteger, PactInteger>(new PactInteger(), new PactInteger());
		}
		
	}
	
}
