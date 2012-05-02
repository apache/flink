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
package eu.stratosphere.sopremo;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.testing.SopremoTestPlanTest.Identity;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 *
 */
public class SourceTest2 {
	
	@Test
	public void testAnnotator() {
		
		
		Identity identity = new Identity();
		Source source = new Source(new ConstantExpression(2));
		Sink sink = new Sink();

		sink.setInput(0, identity);
		identity.setInput(0, source);

		SopremoPlan plan = new SopremoPlan();
		plan.setContext(new EvaluationContext());
		plan.setSinks(sink);

		TestPlan testPlan = new TestPlan(plan.assemblePact());
		testPlan.run();
		
	}

}
