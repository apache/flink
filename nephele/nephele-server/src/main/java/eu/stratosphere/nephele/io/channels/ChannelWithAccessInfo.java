/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.nio.channels.FileChannel;

interface ChannelWithAccessInfo {

	FileChannel getChannel();

	FileChannel getAndIncrementReferences();

	/**
	 * Increments the references to this channel. Returns <code>true</code>, if successful, and <code>false</code>,
	 * if the channel has been disposed in the meantime.
	 * 
	 * @return True, if successful, false, if the channel has been disposed.
	 */
	boolean incrementReferences();

	ChannelWithPosition reserveWriteSpaceAndIncrementReferences(int spaceToReserve);

	/**
	 * Decrements the number of references to this channel. If the number of references is zero after the
	 * decrement, the channel is deleted.
	 * 
	 * @return The number of references remaining after the decrement.
	 * @throws IllegalStateException
	 *         Thrown, if the number of references is already zero or below.
	 */
	int decrementReferences();

	/**
	 * Disposes the channel without further notice. Tries to close it (swallowing all exceptions) and tries
	 * to delete the file.
	 */
	void disposeSilently();

	/**
	 * Updates the flag which indicates whether the underlying physical file shall be deleted when it is closed. Once
	 * the flag was updated to <code>false</code> it cannot be set to <code>true</code> again.
	 * 
	 * @param deleteOnClose
	 *        <code>true</code> to indicate the file shall be deleted when closed, <code>false</code> otherwise
	 */
	void updateDeleteOnCloseFlag(final boolean deleteOnClose);
}
