/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.type.base;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

public class KeyValuePairTest {

	@Mock
	Key myKey;

	@Mock
	Value myValue;

	private DataInputStream in;

	private DataOutputStream out;

	@Before
	public void setUp() {
		initMocks(this);
		try {
			PipedInputStream input = new PipedInputStream(1000);
			in = new DataInputStream(input);
			out = new DataOutputStream(new PipedOutputStream(input));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testKeyValuePair() throws IOException {
		KeyValuePair<Key, Value> toTest = new KeyValuePair<Key, Value>();
		toTest.setKey(myKey);
		toTest.setValue(myValue);

		when(myKey.toString()).thenReturn("");
		toTest.write(out);
		verify(myKey, times(1)).write(Matchers.any(DataOutput.class));
		verify(myValue, times(1)).write(Matchers.any(DataOutput.class));

		toTest.read(in);
		verify(myKey, times(1)).read(Matchers.any(DataInput.class));
		verify(myValue, times(1)).read(Matchers.any(DataInput.class));
	}

}
