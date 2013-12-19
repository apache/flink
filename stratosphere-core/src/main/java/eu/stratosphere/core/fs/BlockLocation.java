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

package eu.stratosphere.core.fs;

import java.io.IOException;

/**
 * A BlockLocation lists hosts, offset and length
 * of block.
 * 
 */
public interface BlockLocation extends Comparable<BlockLocation> {

	/**
	 * Get the list of hosts (hostname) hosting this block.
	 * 
	 * @return a list of hosts (hostname) hosting this block
	 * @throws IOException
	 *         thrown if the list of hosts could not be retrieved
	 */
	String[] getHosts() throws IOException;

	/**
	 * Get the start offset of the file associated with this block.
	 * 
	 * @return the start offset of the file associated with this block
	 */
	long getOffset();

	/**
	 * Get the length of the block.
	 * 
	 * @return the length of the block
	 */
	long getLength();

}
