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

package eu.stratosphere.pact.runtime.util;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.task.util.LastRepeatableIterator;


/**
* Utility class that turns a standard {@link java.util.Iterator} for {@link PactRecord}s into a
* {@link LastRepeatableIterator}.
* 
*  @author Stephan Ewen
*/
public class PactRecordRepeatableIterator implements LastRepeatableIterator<PactRecord>
{
	private final PactRecord copy;
	
	private final MutableObjectIterator<PactRecord> input;
	
	// --------------------------------------------------------------------------------------------
	
	public PactRecordRepeatableIterator(MutableObjectIterator<PactRecord> input)
	{
		this.input = input;
		this.copy = new PactRecord();
	}
	
	public PactRecordRepeatableIterator(Iterator<PactRecord> input)
	{
		this.input = new MutableObjectIteratorWrapper<PactRecord>(input);
		this.copy = new PactRecord();
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public PactRecord next(PactRecord target) throws IOException
	{
		PactRecord localNext = this.input.next(target);
		if (localNext != null) {
			localNext.copyTo(this.copy);
		}
		return localNext;
	}

	@Override
	public PactRecord repeatLast(PactRecord target)
	{
		this.copy.copyTo(target);
		return target;
	}
}