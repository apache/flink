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

package eu.stratosphere.streaming.partitioner;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.types.Key;

//Grouping by a key
public class FieldsPartitioner implements ChannelSelector<StreamRecord> {

	private int keyPosition;
	private Class<? extends Key> keyClass;

	// TODO: make sure it is actually a key
	public FieldsPartitioner(int keyPosition, Class<? extends Key> keyClass) {
		this.keyPosition = keyPosition;
		this.keyClass = keyClass;
	}

	@Override
	public int[] selectChannels(StreamRecord record, int numberOfOutputChannels) {
		Key key = null;
		try {
			key = keyClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		key = keyClass.cast(record.getField(keyPosition));
		return new int[] { Math.abs(key.hashCode()) % numberOfOutputChannels };
	}
}
