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

package eu.stratosphere.test.testPrograms.util;

import java.io.IOException;

import eu.stratosphere.api.java.record.io.GenericInputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;

/**
 * 
 */
public class InfiniteIntegerInputFormat extends GenericInputFormat {
	private static final long serialVersionUID = 1L;
	
	private final IntValue one = new IntValue(1);
	

	@Override
	public boolean reachedEnd() throws IOException {
		return false;
	}


	@Override
	public Record nextRecord(Record record) throws IOException{
		record.setField(0, this.one);
		return record;
	}
}
