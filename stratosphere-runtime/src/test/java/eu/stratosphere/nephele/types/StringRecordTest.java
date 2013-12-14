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

package eu.stratosphere.nephele.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.DataInput;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.util.CommonTestUtils;


/**
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *         TODO: {@link StringRecord} has a lot of public methods that need to be tested.
 */
public class StringRecordTest {

	@Mock
	private DataInput inputMock;

	@Before
	public void setUp() {
		initMocks(this);
	}

	/**
	 * Tests the serialization/deserialization of the {@link StringRecord} class.
	 */
	@Test
	public void testStringRecord() {

		final StringRecord orig = new StringRecord("Test Record");

		try {

			final StringRecord copy = (StringRecord) CommonTestUtils.createCopy(orig);
			
			assertEquals(orig.getLength(), copy.getLength());
			assertEquals(orig.toString(), copy.toString());
			assertEquals(orig, copy);
			assertEquals(orig.hashCode(), copy.hashCode());

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	@Test
	public void shouldReadProperInputs() {
		try {

			when(this.inputMock.readBoolean()).thenReturn(true);
			when(this.inputMock.readInt()).thenReturn(10);

			final String readString = StringRecord.readString(inputMock);
			assertThat(readString, is(not(nullValue())));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}

	}

	@Test
	public void shouldReadNegativeInputs() {
		
		try {
		
			when(this.inputMock.readBoolean()).thenReturn(true);
			when(this.inputMock.readInt()).thenReturn(-1);
		} catch(IOException ioe) {
			fail(ioe.getMessage());
		}

		try {
			StringRecord.readString(inputMock);
		} catch(IOException ioe) {
			return;
		}

		fail("StringRecord.readString did not throw an IOException for negative length of string");		
	}
}
