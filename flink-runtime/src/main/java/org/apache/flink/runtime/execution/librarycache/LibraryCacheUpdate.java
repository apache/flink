/*
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

package org.apache.flink.runtime.execution.librarycache;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * This class is used to encapsulate the transmission of a library file in a RPC call.
 */
public class LibraryCacheUpdate implements IOReadableWritable {

	/** The name of the library file that is transmitted with this object. */
	private String libraryFileName;

	/**
	 * Constructs a new library cache update object.
	 * 
	 * @param libraryFileName
	 *        the name of the library that should be transported within this object.
	 */
	public LibraryCacheUpdate(String libraryFileName) {
		if (libraryFileName == null) {
			throw new IllegalArgumentException("libraryFileName must not be null");
		}
		
		this.libraryFileName = libraryFileName;
	}

	/**
	 * Constructor used to reconstruct the object at the receiver of the RPC call.
	 */
	public LibraryCacheUpdate() {}


	@Override
	public void read(DataInputView in) throws IOException {
		LibraryCacheManager.readLibraryFromStream(in);
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		if (this.libraryFileName == null) {
			throw new IOException("libraryFileName is null");
		}

		LibraryCacheManager.writeLibraryToStream(this.libraryFileName, out);
	}
}
