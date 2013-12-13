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

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public final class DummyInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	private final PactInteger integer = new PactInteger(1);

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
	 */
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		target.setField(0, this.integer);
		target.setField(1, this.integer);
		return true;
	}

	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return (cachedStatistics instanceof FileBaseStatistics) ? (FileBaseStatistics) cachedStatistics : null;
	}
}
