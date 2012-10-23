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

package eu.stratosphere.pact.common.type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PactRecordITCase {
	
	private static final long SEED = 354144423270432543L;
	private final Random rand = new Random(PactRecordITCase.SEED);
	
	private DataInputStream in;
	private DataOutputStream out;

	@Before
	public void setUp() throws Exception
	{
		PipedInputStream pipedInput = new PipedInputStream(32*1024*1024);
		this.in = new DataInputStream(pipedInput);
		this.out = new DataOutputStream(new PipedOutputStream(pipedInput));
	}
	
	@Test
	public void massiveRandomBlackBoxTests()
	{
		try {
			// random test with records with a small number of fields
			for (int i = 0; i < 100000; i++) {
				final Value[] fields = PactRecordTest.createRandomValues(this.rand, 0, 32);
				PactRecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
			}
			
			// random tests with records with a moderately large number of fields
			for (int i = 0; i < 2000; i++) {
				final Value[] fields = PactRecordTest.createRandomValues(this.rand, 20, 200);
				PactRecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
			}
			
			// random tests with records with very many fields
			for (int i = 0; i < 200; i++) {
				final Value[] fields = PactRecordTest.createRandomValues(this.rand, 500, 2000);
				PactRecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
			}
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}
}
