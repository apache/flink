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

package org.apache.flink.mesos.util;

import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * A generic Mesos artifact server, designed specifically for use by the Mesos Fetcher.
 *
 * <p>More information:
 * http://mesos.apache.org/documentation/latest/fetcher/
 * http://mesos.apache.org/documentation/latest/fetcher-cache-internals/
 */
public interface MesosArtifactServer extends MesosArtifactResolver {

	/**
	 * Adds a path to the artifact server.
	 * @param path the qualified FS path to serve (local, hdfs, etc).
	 * @param remoteFile the remote path with which to locate the file.
	 * @return the fully-qualified remote path to the file.
	 * @throws MalformedURLException if the remote path is invalid.
	 */
	URL addPath(Path path, Path remoteFile) throws IOException;

	/**
	 * Stops the artifact server.
	 * @throws Exception
	 */
	void stop() throws Exception;
}
