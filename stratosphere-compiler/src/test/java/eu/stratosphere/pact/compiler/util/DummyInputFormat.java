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

package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;

public final class DummyInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	private final IntValue integer = new IntValue(1);

	@Override
	public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
		target.setField(0, this.integer);
		target.setField(1, this.integer);
		return target;
	}

	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return (cachedStatistics instanceof FileBaseStatistics) ? (FileBaseStatistics) cachedStatistics : null;
	}
}
