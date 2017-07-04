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

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.api.common.JobID;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

public interface LibraryCacheManager {

	/**
	 * Retrieves the required jar files and classpaths and returns a user code class loader.
	 * The jar files are identified by their blob keys.
	 *
	 * @param id job ID
	 * @param requiredJarFiles collection of blob keys identifying the required jar files
	 * @param requiredClasspaths collection of classpaths that are added to the user code class loader
	 * @return ClassLoader which can load the user code
	 *
	 * @throws IOException if any error occurs when retrieving the required jar files
	 */
	ClassLoader getClassLoader(JobID id, Collection<BlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
			throws IOException;
}
