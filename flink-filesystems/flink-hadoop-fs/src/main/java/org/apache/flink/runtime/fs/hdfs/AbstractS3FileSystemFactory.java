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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** Base class for S3 file system factories. */
public abstract class AbstractS3FileSystemFactory implements FileSystemFactory {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractS3FileSystemFactory.class);

	/** Name of this factory for logging. */
	private final String name;

	private final HadoopConfigLoader hadoopConfigLoader;

	protected AbstractS3FileSystemFactory(String name, HadoopConfigLoader hadoopConfigLoader) {
		this.name = name;
		this.hadoopConfigLoader = hadoopConfigLoader;
	}

	@Override
	public String getScheme() {
		return "s3";
	}

	@Override
	public void configure(Configuration config) {
		hadoopConfigLoader.setFlinkConfig(config);
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		LOG.debug("Creating S3 file system (backed by " + name + ")");
		LOG.debug("Loading Hadoop configuration for " + name);
		org.apache.hadoop.conf.Configuration hadoopConfig = hadoopConfigLoader.getOrLoadHadoopConfig();
		org.apache.hadoop.fs.FileSystem fs = createHadoopFileSystem();
		fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);
		return new HadoopFileSystem(fs);
	}

	protected abstract org.apache.hadoop.fs.FileSystem createHadoopFileSystem();

	protected abstract URI getInitURI(
		URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig);
}

