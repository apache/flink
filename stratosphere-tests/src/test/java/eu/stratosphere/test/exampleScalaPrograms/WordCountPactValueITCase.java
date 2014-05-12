/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.exampleScalaPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.examples.scala.wordcount.WordCountWithUserDefinedType;


public class WordCountPactValueITCase extends eu.stratosphere.test.recordJobTests.WordCountITCase {

	@Override
	protected Plan getTestJob() {
		WordCountWithUserDefinedType wc = new WordCountWithUserDefinedType();
		return wc.getScalaPlan(4, textPath, resultPath);
	}
}
