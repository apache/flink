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
import eu.stratosphere.examples.scala.relational.RelationalQuery;

import java.util.Locale;

@RunWith(Parameterized.class)
public class RelationalQueryITCase extends eu.stratosphere.test.recordJobTests.TPCHQuery3ITCase {

	public RelationalQueryITCase(Configuration config) {
		super(config);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected Plan getTestJob()  {

		RelationalQuery tpch3 = new RelationalQuery();
		return tpch3.getScalaPlan(
				config.getInteger("dop", 1),
				ordersPath,
				lineitemsPath,
				resultPath,
				'F', 1993, "5");
	}
}
