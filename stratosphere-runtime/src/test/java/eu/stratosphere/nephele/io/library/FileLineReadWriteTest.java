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

package eu.stratosphere.nephele.io.library;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.InputSplitProvider;

/**
 * This class checks the functionality of the {@link FileLineReader} and the {@link FileLineWriter} class.
 * 
 * @author marrus
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileLineReader.class)
public class FileLineReadWriteTest {

	@Mock
	private Environment environment;

	@Mock
	private Configuration conf;

	@Mock
	private RecordReader<StringRecord> recordReader;

	@Mock
	private RecordWriter<StringRecord> recordWriter;

	@Mock
	private InputSplitProvider inputSplitProvider;

	private File file = new File("./tmp");

	/**
	 * Set up mocks
	 * 
	 * @throws IOException
	 */
	@Before
	public void before() throws Exception {

		MockitoAnnotations.initMocks(this);
	}

	/**
	 * remove the temporary file
	 */
	@After
	public void after() {
		this.file.delete();
	}

	/**
	 * Tests the read and write methods
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReadWrite() throws Exception {

		this.file.createNewFile();
		FileLineWriter writer = new FileLineWriter();
		Whitebox.setInternalState(writer, "environment", this.environment);
		Whitebox.setInternalState(writer, "input", this.recordReader);
		when(this.environment.getTaskConfiguration()).thenReturn(this.conf);

		when(this.conf.getString("outputPath", null)).thenReturn(this.file.toURI().toString());
		when(this.recordReader.hasNext()).thenReturn(true, true, true, false);
		StringRecord in = new StringRecord("abc");
		try {
			when(this.recordReader.next()).thenReturn(in);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		} catch (InterruptedException e) {
			fail();
			e.printStackTrace();
		}
		writer.invoke();

		final FileInputSplit split = new FileInputSplit(0, new Path(this.file.toURI().toString()), 0,
			this.file.length(), null);
		when(this.environment.getInputSplitProvider()).thenReturn(this.inputSplitProvider);
		when(this.inputSplitProvider.getNextInputSplit()).thenReturn(split, (FileInputSplit) null);

		FileLineReader reader = new FileLineReader();
		Whitebox.setInternalState(reader, "environment", this.environment);
		Whitebox.setInternalState(reader, "output", this.recordWriter);
		StringRecord record = mock(StringRecord.class);

		whenNew(StringRecord.class).withNoArguments().thenReturn(record);

		reader.invoke();

		// verify the correct bytes have been written and read
		verify(record, times(3)).set(in.getBytes());
	}
}
