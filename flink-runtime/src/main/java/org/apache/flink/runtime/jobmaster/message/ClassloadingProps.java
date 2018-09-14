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

package org.apache.flink.runtime.jobmaster.message;

import org.apache.flink.runtime.blob.PermanentBlobKey;

import java.io.Serializable;
import java.net.URL;
import java.util.Collection;

/**
 * The response of classloading props request to JobManager.
 */
public class ClassloadingProps implements Serializable {

	private static final long serialVersionUID = -3282341310808511823L;

	private final int blobManagerPort;

	private final Collection<PermanentBlobKey> requiredJarFiles;

	private final Collection<URL> requiredClasspaths;

	/**
	 * Constructor of ClassloadingProps.
	 *
	 * @param blobManagerPort    The port of the blobManager
	 * @param requiredJarFiles   The blob keys of the required jar files
	 * @param requiredClasspaths The urls of the required classpaths
	 */
	public ClassloadingProps(
		final int blobManagerPort,
		final Collection<PermanentBlobKey> requiredJarFiles,
		final Collection<URL> requiredClasspaths)
	{
		this.blobManagerPort = blobManagerPort;
		this.requiredJarFiles = requiredJarFiles;
		this.requiredClasspaths = requiredClasspaths;
	}

	public int getBlobManagerPort() {
		return blobManagerPort;
	}

	public Collection<PermanentBlobKey> getRequiredJarFiles() {
		return requiredJarFiles;
	}

	public Collection<URL> getRequiredClasspaths() {
		return requiredClasspaths;
	}
}
