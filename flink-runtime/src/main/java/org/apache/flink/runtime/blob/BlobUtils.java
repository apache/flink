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

package org.apache.flink.runtime.blob;

import com.google.common.io.BaseEncoding;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobID;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class BlobUtils {
	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	/**
	 * The prefix of all BLOB files stored by the BLOB server.
	 */
	private static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The prefix of all job-specific directories created by the BLOB server.
	 */
	private static final String JOB_DIR_PREFIX = "job_";

	/**
	 * The default character set to translate between characters and bytes.
	 */
	static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");


	/**
	 * Creates a storage directory for a blob service.
	 *
	 * @return the storage directory used by a BLOB service
	 */
	static File initStorageDirectory() {
		File baseDir;
		String sd = GlobalConfiguration.getString(
				ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		if (sd != null) {
			baseDir = new File(sd);
		} else {
			baseDir = new File(System.getProperty("java.io.tmpdir"));
		}

		File storageDir;
		final int MAX_ATTEMPTS = 10;
		int attempt;

		for(attempt = 0; attempt < MAX_ATTEMPTS; attempt++){
			storageDir = new File(baseDir, String.format(
					"blobStore-%s", UUID.randomUUID().toString()));

			if(!storageDir.exists()){
				// Create the storage directory
				storageDir.mkdirs();

				return storageDir;
			}
		}

		// max attempts exceeded to find a storage directory
		throw new RuntimeException("Could not create a storage directory");
	}

	/**
	 * Returns the BLOB service's directory for incoming files. The directory is created if it did
	 * not exist so far.
	 *
	 * @return the BLOB server's directory for incoming files
	 */
	static File getIncomingDirectory(File storageDir) {
		final File incomingDirectory = new File(storageDir, "incoming");
		incomingDirectory.mkdir();

		return incomingDirectory;
	}

	/**
	 * Returns the BLOB service's directory for cached files. The directory is created if it did
	 * not exist so far.
	 *
	 * @return the BLOB server's directory for cached files
	 */
	private static File getCacheDirectory(File storageDir) {
		final File cacheDirectory = new File(storageDir, "cache");
		cacheDirectory.mkdir();

		return cacheDirectory;
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given key.
	 *
	 * @param key
	 *        the key identifying the BLOB
	 * @return the (designated) physical storage location of the BLOB
	 */
	static File getStorageLocation(final File storageDir,  final BlobKey key) {
		return new File(getCacheDirectory(storageDir), BLOB_FILE_PREFIX + key.toString());
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given job ID and key.
	 *
	 * @param jobID
	 *        the ID of the job the BLOB belongs to
	 * @param key
	 *        the key of the BLOB
	 * @return the (designated) physical storage location of the BLOB with the given job ID and key
	 */
	static File getStorageLocation(final File storageDir, final JobID jobID,
									final String key) {

		return new File(getJobDirectory(storageDir, jobID), BLOB_FILE_PREFIX + encodeKey(key));
	}

	/**
	 * Returns the BLOB server's storage directory for BLOBs belonging to the job with the given ID.
	 *
	 * @param jobID
	 *        the ID of the job to return the storage directory for
	 * @return the storage directory for BLOBs belonging to the job with the given ID
	 */
	private static File getJobDirectory(final File storageDir, final JobID jobID){
		final File jobDirectory = new File(storageDir, JOB_DIR_PREFIX + jobID.toString());
		jobDirectory.mkdirs();

		return jobDirectory;
	}

	/**
	 * Translates the user's key for a BLOB into the internal name used by the BLOB server
	 *
	 * @param key
	 *        the user's key for a BLOB
	 * @return the internal name for the BLOB as used by the BLOB server
	 */
	private static String encodeKey(final String key) {

		return BaseEncoding.base64().encode(key.getBytes(DEFAULT_CHARSET));
	}

	/**
	 * Deletes the storage directory for the job with the given ID.
	 *
	 * @param jobID
	 *			jobID whose directory shall be deleted
	 */
	static void deleteJobDirectory(final File storageDir, final JobID jobID) throws IOException {
		File directory = getJobDirectory(storageDir, jobID);

		FileUtils.deleteDirectory(directory);
	}

	/**
	 * Creates a new instance of the message digest to use for the BLOB key computation.
	 *
	 * @return a new instance of the message digest to use for the BLOB key computation
	 */
	static MessageDigest createMessageDigest() {
		try {
			return MessageDigest.getInstance(HASHING_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
