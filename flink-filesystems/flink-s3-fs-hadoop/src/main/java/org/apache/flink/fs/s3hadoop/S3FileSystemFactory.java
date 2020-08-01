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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Collections;

/**
 * Simple factory for the S3 file system.
 */
public class S3FileSystemFactory extends AbstractS3FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemFactory.class);

	private static final String[] FLINK_CONFIG_PREFIXES = { "s3.", "s3a.", "fs.s3a." };

	private static final String[][] MIRRORED_CONFIG_KEYS = {
			{ "fs.s3a.access-key", "fs.s3a.access.key" },
			{ "fs.s3a.secret-key", "fs.s3a.secret.key" },
			{ "fs.s3a.path-style-access", "fs.s3a.path.style.access" }
	};

	public S3FileSystemFactory() {
		super("Hadoop s3a file system", createHadoopConfigLoader());
	}

	@Override
	public String getScheme() {
		return "s3";
	}

	@VisibleForTesting
	static HadoopConfigLoader createHadoopConfigLoader() {
		return new HadoopConfigLoader(FLINK_CONFIG_PREFIXES, MIRRORED_CONFIG_KEYS,
			"fs.s3a.", Collections.emptySet(), Collections.emptySet(), "");
	}

	@Override
	protected org.apache.hadoop.fs.FileSystem createHadoopFileSystem() {
		return new S3AFileSystem();
	}

	@Override
	protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
		final String scheme = fsUri.getScheme();
		final String authority = fsUri.getAuthority();

		if (scheme == null && authority == null) {
			fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
		}
		else if (scheme != null && authority == null) {
			URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
			if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
				fsUri = defaultUri;
			}
		}

		LOG.debug("Using scheme {} for s3a file system backing the S3 File System", fsUri);

		return fsUri;
	}

	@Nullable
	@Override
	protected S3AccessHelper getS3AccessHelper(FileSystem fs) {
		final S3AFileSystem s3Afs = (S3AFileSystem) fs;
		return new HadoopS3AccessHelper(s3Afs, s3Afs.getConf());
	}
}
