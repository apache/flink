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

package eu.stratosphere.sopremo.execution;

import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.protocols.VersionedProtocol;

/**
 * The library transfer protocol allows to query servers for cached libraries and submit these if necessary.
 * 
 * @author warneke
 * @author Arvid Heise
 */
public interface LibraryTransferProtocol extends VersionedProtocol {
	/**
	 * Queries the task manager about the cache status of the libraries stated in the {@link LibraryCacheProfileRequest}
	 * object.
	 * 
	 * @param request
	 *        a {@link LibraryCacheProfileRequest} containing a list of libraries whose cache status is to be determined
	 * @return a {@link LibraryCacheProfileResponse} containing the cache status for each library included in the
	 *         request
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	LibraryCacheProfileResponse getLibraryCacheProfile(LibraryCacheProfileRequest request) throws IOException;

	/**
	 * Updates the task manager's library cache.
	 * 
	 * @param update
	 *        a {@link LibraryCacheUpdate} object used to transmit the library data
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void updateLibraryCache(LibraryCacheUpdate update) throws IOException;
}
