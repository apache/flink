/**
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


package org.apache.flink.api.java.record.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.junit.Test;

public class TextInputFormatTest {
	/**
	 * The TextInputFormat seems to fail reading more than one record. I guess its
	 * an off by one error.
	 * 
	 * The easiest workaround is to setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
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
			
			FileWriter writer = new FileWriter(tempFile);
			writer.append(FIRST).append('\n');
			writer.append(SECOND).append('\n');
			writer.close();
			
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(tempFile.toURI().toString());
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);
			
			inputFormat.open(splits[0]);
			
			Record r = new Record();
			assertNotNull("Expecting first record here", inputFormat.nextRecord(r));
			assertEquals(FIRST, r.getField(0, StringValue.class).getValue());
			
			assertNotNull("Expecting second record here",inputFormat.nextRecord(r ));
			assertEquals(SECOND, r.getField(0, StringValue.class).getValue());
			
			assertNull("The input file is over", inputFormat.nextRecord(r));
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
			
			TextInputFormat inputFormat = new TextInputFormat();
			inputFormat.setFilePath(tempFile.toURI().toString());
			
			Configuration parameters = new Configuration(); 
			inputFormat.configure(parameters);
			
			inputFormat.setDelimiter(delimiter);
			
			FileInputSplit[] splits = inputFormat.createInputSplits(1);
						
			inputFormat.open(splits[0]);
			
			Record r = new Record();
			if (  (delimiter.equals("\n") && (lineBreaker.equals("\n") || lineBreaker.equals("\r\n") ) ) 
					|| (lineBreaker.equals(delimiter)) ){

				assertNotNull("Expecting first record here", inputFormat.nextRecord(r));
				assertEquals(FIRST, r.getField(0, StringValue.class).getValue());
				
				assertNotNull("Expecting second record here",inputFormat.nextRecord(r ));
				assertEquals(SECOND, r.getField(0, StringValue.class).getValue());
				
				assertNull("The input file is over", inputFormat.nextRecord(r));
			}else{
				assertNotNull("Expecting first record here", inputFormat.nextRecord(r));
				assertEquals(CONTENT, r.getField(0, StringValue.class).getValue());
			}
			
			
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

}
