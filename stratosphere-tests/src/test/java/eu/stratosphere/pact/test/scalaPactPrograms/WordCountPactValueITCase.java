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

import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.configuration.Configuration;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import eu.stratosphere.scala.examples.wordcount.WordCountPactValue;

@RunWith(Parameterized.class)
public class WordCountPactValueITCase extends eu.stratosphere.pact.test.pactPrograms.WordCountITCase {

    public WordCountPactValueITCase(Configuration config) {
        super(config);
    }

    @Override
    protected Plan getPactPlan() {
        WordCountPactValue wc = new WordCountPactValue();
        return wc.getScalaPlan(
                config.getInteger("WordCountTest#NumSubtasks", 1),
                textPath, resultPath);
    }


}
