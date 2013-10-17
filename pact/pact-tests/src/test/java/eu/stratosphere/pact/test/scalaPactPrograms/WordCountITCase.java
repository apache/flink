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

package eu.stratosphere.pact.test.scalaPactPrograms;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.scala.examples.wordcount.WordCount;

@RunWith(Parameterized.class)
public class WordCountITCase extends eu.stratosphere.pact.test.pactPrograms.WordCountITCase {

	public WordCountITCase(Configuration config) {
		super(config);
	}

	@Override
	protected Plan getPactPlan() {
		WordCount wc = new WordCount();
		Plan plan =  wc.getPlan(textPath, resultPath);
		plan.setDefaultParallelism(config.getInteger("WordCountTest#NumSubtasks", 1));
		return plan;
	}

	
}
