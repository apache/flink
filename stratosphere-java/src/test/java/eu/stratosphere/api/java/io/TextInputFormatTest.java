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

package eu.stratosphere.api.java.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
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
			
			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);
			
			inputFormat.open(splits[0]);
			
			String result = "";
			
			result = inputFormat.nextRecord("");
			assertNotNull("Expecting first record here", result);
			assertEquals(FIRST, result);
			
			result = inputFormat.nextRecord(result);
			assertNotNull("Expecting second record here", result);
			assertEquals(SECOND, result);
			
			result = inputFormat.nextRecord(result);
			assertNull("The input file is over", result);
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}
	
	/**
	 * This tests cases when line ends with \r\n and \n is used as delimiter, the last \r should be removed 
	 */
	@Test
	public void testRemovingTrailingCR() {
		
		testRemovingTrailingCR("\n","\n");
		testRemovingTrailingCR("\r\n","\n");
		
		testRemovingTrailingCR("|","|");
		testRemovingTrailingCR("|","\n");
	}
	
	private void testRemovingTrailingCR(String lineBreaker,String delimiter) {
		File tempFile=null;
		
		String FIRST = "First line";
		String SECOND = "Second line";
		String CONTENT = FIRST + lineBreaker + SECOND + lineBreaker;
		
		try {
			// create input file
			tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);
			
			OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
			wrt.write(CONTENT);
			wrt.close();
			
			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			inputFormat.setFilePath(tempFile.toURI().toString());
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			inputFormat.setDelimiter(delimiter);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
						
			inputFormat.open(splits[0]);
			

			String result = "";
			if (  (delimiter.equals("\n") && (lineBreaker.equals("\n") || lineBreaker.equals("\r\n") ) ) 
					|| (lineBreaker.equals(delimiter)) ){
				
				result = inputFormat.nextRecord("");
				assertNotNull("Expecting first record here", result);
				assertEquals(FIRST, result);
				
				result = inputFormat.nextRecord(result);
				assertNotNull("Expecting second record here", result);
				assertEquals(SECOND, result);
				
				result = inputFormat.nextRecord(result);
				assertNull("The input file is over", result);
				
			}else{
				result = inputFormat.nextRecord("");
				assertNotNull("Expecting first record here", result);
				assertEquals(CONTENT, result);
			}
			
			
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

}
