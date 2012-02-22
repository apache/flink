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
package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 *
 */
public class ArraySchema implements Schema{

	/**
	 * 
	 */
	private static final long serialVersionUID = 415254957689311752L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#getPactSchema()
	 */
	@Override
	public Class<? extends Value>[] getPactSchema() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		// TODO auto generated method stub
		return null;
	}

}
