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

import java.io.Serializable;

import eu.stratosphere.api.record.functions.CrossFunction;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

public class DummyCrossStub extends CrossFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.api.record.functions.CrossFunction#cross(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.api.record.functions.Collector)
	 */
	@Override
	public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
		out.collect(record1);
		out.collect(record2);
	}
}
