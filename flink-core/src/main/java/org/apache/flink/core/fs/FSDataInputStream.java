/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package org.apache.flink.core.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for a data input stream to a file on a {@link FileSystem}.
 * 
 */
public abstract class FSDataInputStream extends InputStream {

	/**
	 * Seek to the given offset from the start of the file. The next read() will be from that location.
	 * Can't seek past the end of the file.
	 * 
	 * @param desired
	 *        the desired offset
	 * @throws IOException
	 *         thrown if an error occurred while seeking inside the input stream
	 */
	public abstract void seek(long desired) throws IOException;

}
