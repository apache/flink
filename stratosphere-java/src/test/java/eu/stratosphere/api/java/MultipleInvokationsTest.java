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

package eu.stratosphere.api.java;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.java.io.DiscardingOuputFormat;

public class MultipleInvokationsTest {

	@Test
	public void testMultipleInvocationsGetPlan() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			// ----------- Execution 1 ---------------
			
			DataSet<String> data = env.fromElements("Some", "test", "data").name("source1");
			data.print().name("print1");
			data.output(new DiscardingOuputFormat<String>()).name("output1");
			
			{
				Plan p = env.createProgramPlan();
				
				assertEquals(2, p.getDataSinks().size());
				for (GenericDataSinkBase<?> sink : p.getDataSinks()) {
					assertTrue(sink.getName().equals("print1") || sink.getName().equals("output1"));
					assertEquals("source1", sink.getInput().getName());
				}
			}
			
			// ----------- Execution 2 ---------------
			
			data.writeAsText("/some/file/path").name("textsink");
			
			{
				Plan p = env.createProgramPlan();
			
				assertEquals(1, p.getDataSinks().size());
				GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
				assertEquals("textsink", sink.getName());
				assertEquals("source1", sink.getInput().getName());
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
