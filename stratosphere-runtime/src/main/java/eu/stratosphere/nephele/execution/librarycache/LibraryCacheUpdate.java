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

package eu.stratosphere.nephele.execution.librarycache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;

/**
 * This class is used to encapsulate the transmission of a library file in a Nephele RPC call.
 * 
 * @author warneke
 */
public class LibraryCacheUpdate implements IOReadableWritable {

	/**
	 * The name of the library file that is transmitted with this object.
	 */
	private String libraryFileName = null;

	/**
	 * Constructs a new library cache update object.
	 * 
	 * @param libraryFileName
	 *        the name of the library that should be transported within this object.
	 */
	public LibraryCacheUpdate(final String libraryFileName) {
		this.libraryFileName = libraryFileName;
	}

	/**
	 * Constructor used to reconstruct the object at the receiver of the RPC call.
	 */
	public LibraryCacheUpdate() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		LibraryCacheManager.readLibraryFromStream(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.libraryFileName == null) {
			throw new IOException("libraryFileName is null");
		}

		LibraryCacheManager.writeLibraryToStream(this.libraryFileName, out);
	}

}
