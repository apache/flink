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

package eu.stratosphere.api.java.record.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.LogUtils;

public class TextInputFormatTest {
	
	@BeforeClass
	public static void initialize() {
		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	/**
	 * The TextInputFormat seems to fail reading more than one record. I guess its
	 * an off by one error.
	 * 
	 * The easiest workaround is to setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
	 * @throws IOException
	 */
	@Test
	public void testPositionBug() {
		final String FIRST = "First line";
		final String SECOND = "Second line";
		
		try {
			// create input file
			File tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);
			
			PrintStream ps = new  PrintStream(tempFile);
			ps.println(FIRST);
			ps.println(SECOND);
			ps.close();
			
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(tempFile.toURI().toString());
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);
			
			inputFormat.open(splits[0]);
			
			Record r = new Record();
			assertTrue("Expecting first record here", inputFormat.nextRecord(r));
			assertEquals(FIRST, r.getField(0, StringValue.class).getValue());
			
			assertTrue("Expecting second record here",inputFormat.nextRecord(r ));
			assertEquals(SECOND, r.getField(0, StringValue.class).getValue());
			
			assertFalse("The input file is over", inputFormat.nextRecord(r));
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}
}
