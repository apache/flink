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

package eu.stratosphere.nephele.io.channels;

import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;

/**
 * A {@link ChannelCanceledException} is thrown to indicate that the content which is about to be written/or read is
 * destined to an {@link AbstractByteBufferedInputChannel} which is already canceled, i.e. whose task has been aborted.
 * 
 * @author warneke
 */
public class ChannelCanceledException extends Exception {

	/**
	 * The generated serial version UID
	 */
	private static final long serialVersionUID = 4755984018695543899L;

}
