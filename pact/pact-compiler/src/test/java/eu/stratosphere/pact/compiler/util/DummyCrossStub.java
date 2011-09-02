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

package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class DummyCrossStub extends CrossStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

	@Override
	public void cross(PactInteger key1, PactInteger value1, PactInteger key2, PactInteger value2,
			Collector<PactInteger, PactInteger> out) {
		out.collect(key1, value1);
		out.collect(key2, value2);
	}

}
