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
package eu.stratosphere.pact.common.type.stub;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Value;

/**
 * Needed to use the mock implementations of {@link ReduceStub} and {@link MatchStub}.
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
class TupleMock implements Value
{
	private String name = "";
	
	public TupleMock()
	{}
	
	public TupleMock(String name)
	{
		this.name = name;
	}
	
	@Override
	public void read(DataInput in) throws IOException {
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}
	
	@Override
	public String toString() {
		return this.name;
	}
	
}