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

package eu.stratosphere.nephele.io;

import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.types.Record;

/**
 * Objects implementing this interface are passed to an {@link OutputGate}. When a record is sent through the output
 * gate, the channel selector object is called to determine to which {@link AbstractOutputChannel} objects the record
 * shall be passed on.
 * 
 * @author warneke
 * @param <T>
 *        the type of record which is sent through the attached output gate
 */
public interface ChannelSelector<T extends Record> extends IOReadableWritable {

	/**
	 * This method is called to determine to which attached {@link AbstractOutputChannel} objects the given record shall
	 * be forwarded. The second parameter of this method is a boolean array. The length of the array corresponds to the
	 * number of output channels attached to the output gate. When this method is called, all fields of the array are
	 * set to <code>false</code>. To indicate that the given record shall be forwarded to the i-th channel of the output
	 * gate, set the i-th field of the array to <code>true</code>. It is possible to set multiple array fields to
	 * <code>true</code>.
	 * 
	 * @param record
	 *        the record for which the output channels shall be determined
	 * @param channelFlags
	 *        an array of booleans indicating what output shall be used for the record
	 */
	void selectChannels(T record, boolean[] channelFlags);
}
