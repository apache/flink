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

package eu.stratosphere.pact.test.testPrograms.util;

import java.io.IOException;

import eu.stratosphere.api.record.io.GenericInputFormat;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;

/**
 * 
 */
public class InfiniteIntegerInputFormatWithDelay extends GenericInputFormat {
	private static final long serialVersionUID = 1L;
	
	private static final int DELAY = 20;
	
	private final PactInteger one = new PactInteger(1);
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#reachedEnd()
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException
	{
		record.setField(0, this.one);
		
		try {
			Thread.sleep(DELAY);
		} catch (InterruptedException iex) {}
		
		return true;
	}
}
