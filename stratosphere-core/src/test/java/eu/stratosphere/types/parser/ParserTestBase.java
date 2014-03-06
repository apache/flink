/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.types.parser;

import static org.junit.Assert.*;

import org.junit.Test;


/**
 *
 */
public abstract class ParserTestBase<T> {
	
	public abstract String[] getValidTestValues();
	
	public abstract T[] getValidTestResults();
	
	public abstract String[] getInvalidTestValues();
	
	public abstract FieldParser<T> getParser();
	
	public abstract Class<T> getTypeClass();
	

	@Test
	public void testTest() {
		assertNotNull(getParser());
		assertNotNull(getTypeClass());
		assertNotNull(getValidTestValues());
		assertNotNull(getValidTestResults());
		assertNotNull(getInvalidTestValues());
		assertTrue(getValidTestValues().length == getValidTestResults().length);
	}
	
	@Test
	public void testGetValue() {
		try {
			FieldParser<?> parser = getParser();
			Object created = parser.createValue();
			
			assertNotNull("Null type created", created);
			assertTrue("Wrong type created", getTypeClass().isAssignableFrom(created.getClass()));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testValidStringInIsolation() {
		try {
			String[] testValues = getValidTestValues();
			T[] results = getValidTestResults();
			
			for (int i = 0; i < testValues.length; i++) {
				
				FieldParser<T> parser = getParser();
				
				byte[] bytes = testValues[i].getBytes();
				int numRead = parser.parseField(bytes, 0, bytes.length, '|', parser.createValue());
				
				assertTrue("Parser declared the valid value " + testValues[i] + " as invalid.", numRead != -1);
				assertEquals("Invalid number of bytes read returned.", bytes.length, numRead);
				
				T result = parser.getLastResult();
				
				assertEquals("Parser parsed wrong.", results[i], result);
			}
			
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testConcatenated() {
		try {
			String[] testValues = getValidTestValues();
			T[] results = getValidTestResults();
			
			byte[] allBytesWithDelimiter = concatenate(testValues, '|', true);
			byte[] allBytesNoDelimiterEnd = concatenate(testValues, ',', false);
			
			FieldParser<T> parser1 = getParser();
			FieldParser<T> parser2 = getParser();
			
			T val1 = parser1.createValue();
			T val2 = parser2.createValue();
			
			int pos1 = 0;
			int pos2 = 0;
			
			for (int i = 0; i < results.length; i++) {
				pos1 = parser1.parseField(allBytesWithDelimiter, pos1, allBytesWithDelimiter.length, '|', val1);
				pos2 = parser2.parseField(allBytesNoDelimiterEnd, pos2, allBytesNoDelimiterEnd.length, ',', val2);
				
				assertTrue("Parser declared the valid value " + testValues[i] + " as invalid.", pos1 != -1);
				assertTrue("Parser declared the valid value " + testValues[i] + " as invalid.", pos2 != -1);
				
				T result1 = parser1.getLastResult();
				T result2 = parser2.getLastResult();
				
				assertEquals("Parser parsed wrong.", results[i], result1);
				assertEquals("Parser parsed wrong.", results[i], result2);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testInValidStringInIsolation() {
		try {
			String[] testValues = getInvalidTestValues();
			
			for (int i = 0; i < testValues.length; i++) {
				
				FieldParser<T> parser = getParser();
				
				byte[] bytes = testValues[i].getBytes();
				int numRead = parser.parseField(bytes, 0, bytes.length, '|', parser.createValue());
				
				assertTrue("Parser accepted the invalid value " + testValues[i] + ".", numRead == -1);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testInValidStringsMixedIn() {
		try {
			String[] validValues = getValidTestValues();
			T[] validResults = getValidTestResults();
			
			String[] invalidTestValues = getInvalidTestValues();
			
			FieldParser<T> parser = getParser();
			T value = parser.createValue();
			
			
			for (String invalid : invalidTestValues) {
				
				// place an invalid string in the middle
				String[] testLine = new String[validValues.length + 1];
				int splitPoint = validValues.length / 2;
				System.arraycopy(validValues, 0, testLine, 0, splitPoint);
				testLine[splitPoint] = invalid;
				System.arraycopy(validValues, splitPoint, testLine, splitPoint + 1, validValues.length - splitPoint);
				
				byte[] bytes = concatenate(testLine, '%', true);
				
				// read the valid parts
				int pos = 0;
				for (int i = 0; i < splitPoint; i++) {
					pos = parser.parseField(bytes, pos, bytes.length, '%', value);
					
					assertTrue("Parser declared the valid value " + validValues[i] + " as invalid.", pos != -1);
					T result = parser.getLastResult();
					assertEquals("Parser parsed wrong.", validResults[i], result);
				}
				
				// fail on the invalid part
				pos = parser.parseField(bytes, pos, bytes.length, '%', value);
				assertTrue("Parser accepted the invalid value " + invalid + ".", pos == -1);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}
	
	private static byte[] concatenate(String[] values, char delimiter, boolean delimiterAtEnd) {
		int len = 0;
		for (String s : values) {
			len += s.length() + 1;
		}
		
		if (!delimiterAtEnd) {
			len--;
		}
		
		int currPos = 0;
		byte[] result = new byte[len];
		
		for (int i = 0; i < values.length; i++) {
			String s = values[i];
			
			byte[] bytes = s.getBytes();
			int numBytes = bytes.length;
			System.arraycopy(bytes, 0, result, currPos, numBytes);
			currPos += numBytes;
			
			if (delimiterAtEnd || i < values.length-1) {
				result[currPos++] = (byte) delimiter;
			}
		}
		
		return result;
	}
}
