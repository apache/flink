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

package eu.stratosphere.pact.common.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
public class TextInputFormatTest
{
	@Mock
	protected Configuration config;
	
	protected File tempFile;
	
	private final DelimitedInputFormat format = new MyTextInputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup() {
		initMocks(this);
	}
	
	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
		if (this.tempFile != null) {
			this.tempFile.delete();
		}
	}

	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	@Test
	public void testConfigure() {
		when(this.config.getString(Matchers.matches(DelimitedInputFormat.RECORD_DELIMITER), Matchers.anyString()))
			.thenReturn("\n");
		when(this.config.getString(Matchers.matches(FileInputFormat.FILE_PARAMETER_KEY), Matchers.anyString()))
		.thenReturn("file:///some/file/that/will/not/be/read");
		
		format.configure(this.config);
		verify(this.config, times(3)).getString(Matchers.any(String.class), Matchers.any(String.class));
		assertEquals("\n", new String(format.getDelimiter()));

		when(this.config.getString(Matchers.matches(DelimitedInputFormat.RECORD_DELIMITER), Matchers.anyString()))
			.thenReturn("&-&");
		format.configure(this.config);
		verify(this.config, times(6)).getString(Matchers.any(String.class), Matchers.any(String.class));
		assertEquals("&-&", new String(format.getDelimiter()));
	}

	@Test
	public void testOpen() throws IOException
	{
		final String myString = "my mocked line 1\nmy mocked line 2\n";
		final FileInputSplit split = createTempFile(myString);	
		
		int bufferSize = 5;
		format.setBufferSize(bufferSize);
		format.open(split);
		assertEquals(0, format.getSplitStart());
		assertEquals(myString.length() - bufferSize, format.getSplitLength());
		assertEquals(bufferSize, format.getBufferSize());
	}

	@Test
	public void testRead() throws IOException
	{
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		final FileInputSplit split = createTempFile(myString);
		
		final Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		parameters.setString(DelimitedInputFormat.RECORD_DELIMITER, "$$$");
		
		format.configure(parameters);
		format.open(split);
		
		PactRecord theRecord = new PactRecord();

		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key", theRecord.getField(0, PactString.class).getValue());
		assertEquals("my val", theRecord.getField(1, PactString.class).getValue());
		
		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key2\n$$ctd.$$", theRecord.getField(0, PactString.class).getValue());
		assertEquals("my value2", theRecord.getField(1, PactString.class).getValue());
		
		assertFalse(format.nextRecord(theRecord));
		assertTrue(format.reachedEnd());
	}
	
	@Test
	public void testRead2() throws IOException
	{
		// 2. test case
		final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		final FileInputSplit split = createTempFile(myString);
		
		final Configuration parameters = new Configuration();
		parameters.setString(FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		parameters.setString(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		
		
		format.configure(parameters);
		format.open(split);

		PactRecord theRecord = new PactRecord();

		assertTrue(format.nextRecord(theRecord));
		assertEquals("my key", theRecord.getField(0, PactString.class).getValue());
		assertEquals("my val$$$my key2", theRecord.getField(1, PactString.class).getValue());
		
		assertTrue(format.nextRecord(theRecord));
		assertEquals("$$ctd.$$", theRecord.getField(0, PactString.class).getValue());
		assertEquals("my value2", theRecord.getField(1, PactString.class).getValue());
		
		assertFalse(format.nextRecord(theRecord));
		assertTrue(format.reachedEnd());
	}
	
	private FileInputSplit createTempFile(String contents) throws IOException
	{
		this.tempFile = File.createTempFile("test_contents", "tmp");
		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(this.tempFile));
		wrt.write(contents);
		wrt.close();
		
		return new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	}
	
	private final class MyTextInputFormat extends DelimitedInputFormat
	{
		private final PactString str1 = new PactString();
		private final PactString str2 = new PactString();
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
		 */
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
		{
			String theRecord = new String(bytes, offset, numBytes);
			
			str1.setValue(theRecord.substring(0, theRecord.indexOf('|')));
			str2.setValue(theRecord.substring(theRecord.indexOf('|') + 1));
			
			target.setField(0, str1);
			target.setField(1, str2);
			return true;
		}
	}
}
