/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.test.window.wordcount;

import eu.stratosphere.streaming.api.AtomRecord;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

public class WindowWordCountSource extends UserSourceInvokable {
	private final String motto = "Gyuszi Gabor Big Marci Gyuszi";
	private long timestamp;
	private AtomRecord mottoRecord = null;

	@Override
	public void invoke() throws Exception {
		timestamp = 0;
		for (int i = 0; i < 2; ++i) {
			mottoRecord = new AtomRecord(2);
			mottoRecord.setField(0, new StringValue(motto));
			mottoRecord.setField(1, new LongValue(timestamp));
			emit(new StreamRecord(mottoRecord));
			++timestamp;
		}
	}
}
