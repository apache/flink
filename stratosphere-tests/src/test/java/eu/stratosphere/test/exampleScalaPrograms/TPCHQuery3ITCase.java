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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.examples.scala.relational.TPCHQuery3;

@RunWith(Parameterized.class)
public class TPCHQuery3ITCase extends eu.stratosphere.test.exampleRecordPrograms.TPCHQuery3ITCase {

	public TPCHQuery3ITCase(Configuration config) {
		super(config);
	}

	@Override
	protected Plan getTestJob()  {

		TPCHQuery3 tpch3 = new TPCHQuery3();
		return tpch3.getScalaPlan(
				config.getInteger("TPCHQuery3Test#NoSubtasks", 1),
				ordersPath,
				lineitemsPath,
				resultPath,
				'F', 1993, "5");
	}
}
