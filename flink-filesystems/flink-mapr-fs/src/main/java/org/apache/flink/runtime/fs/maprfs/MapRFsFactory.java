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

package org.apache.flink.runtime.fs.maprfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory for the MapR file system.
 *
 * <p>This factory tries to reflectively instantiate the MapR file system. It can only be
 * used when the MapR FS libraries are in the classpath.
 */
public class MapRFsFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(MapRFsFactory.class);

	// ------------------------------------------------------------------------

	@Override
	public String getScheme() {
		return "maprfs";
	}

	@Override
	public void configure(Configuration config) {
		// nothing to configure based on the configuration here
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "fsUri");

		try {
			LOG.info("Trying to load and instantiate MapR File System");

			return new MapRFileSystem(fsUri);
		}
		catch (LinkageError e) {
			throw new IOException("Could not load MapR file system. "  +
					"Please make sure the Flink runtime classes are part of the classpath or dependencies.", e);
		}
		catch (IOException e) {
			throw e;
		}
		catch (Throwable t) {
			throw new IOException("Could not instantiate MapR file system.", t);
		}
	}
}
