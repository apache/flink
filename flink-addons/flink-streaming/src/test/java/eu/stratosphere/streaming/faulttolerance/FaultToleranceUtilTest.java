/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.faulttolerance;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class FaultToleranceUtilTest {

	FaultToleranceUtil faultTolerancyBuffer;
	List<RecordWriter<StreamRecord>> outputs;

	@Before
	public void setFaultTolerancyBuffer() {
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		int[] numOfOutputchannels = { 1, 2 };
		faultTolerancyBuffer = new FaultToleranceUtil(outputs, 1, numOfOutputchannels);
	}

	@Test
	public void testFaultTolerancyBuffer() {
		
	}

	@Test
	public void testAddRecord() {
		
	}

	@Test
	public void testAddTimestamp() {
		
	}

	@Test
	public void testPopRecord() {
		
	}

	@Test
	public void testRemoveRecord() {
		
	}

	@Test
	public void testAckRecord() {
		
	}

	@Test
	public void testFailRecord() {
		
	}

	// TODO: create more tests for this method
	@Test
	public void testTimeOutRecords() {
		
	}
}
