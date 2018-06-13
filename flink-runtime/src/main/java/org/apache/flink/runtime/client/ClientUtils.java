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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Contains utility methods for clients.
 */
public enum ClientUtils {
	;

	/**
	 * Uploads the user jars from the given {@link JobGraph} using the given {@link BlobClient},
	 * and sets the appropriate blobkeys.
	 *
	 * @param jobGraph   jobgraph requiring user jars
	 * @param blobClient client to upload jars with
	 * @throws IOException if the upload fails
	 */
	public static void uploadAndSetUserJars(JobGraph jobGraph, BlobClient blobClient) throws IOException {
		Collection<PermanentBlobKey> blobKeys = uploadUserJars(jobGraph.getJobID(), jobGraph.getUserJars(), blobClient);
		setUserJarBlobKeys(blobKeys, jobGraph);
	}

	private static Collection<PermanentBlobKey> uploadUserJars(JobID jobId, Collection<Path> userJars, BlobClient blobClient) throws IOException {
		Collection<PermanentBlobKey> blobKeys = new ArrayList<>(userJars.size());
		for (Path jar : userJars) {
			final PermanentBlobKey blobKey = blobClient.uploadFile(jobId, jar);
			blobKeys.add(blobKey);
		}
		return blobKeys;
	}

	private static void setUserJarBlobKeys(Collection<PermanentBlobKey> blobKeys, JobGraph jobGraph) {
		blobKeys.forEach(jobGraph::addUserJarBlobKey);
	}

	/**
	 * Uploads the user artifacts from the given {@link JobGraph} using the given {@link BlobClient},
	 * and sets the appropriate blobkeys.
	 *
	 * @param jobGraph jobgraph requiring user artifacts
	 * @param blobClient client to upload artifacts with
	 * @throws IOException if the upload fails
	 */
	public static void uploadAndSetUserArtifacts(JobGraph jobGraph, BlobClient blobClient) throws IOException {
		Collection<Tuple2<String, PermanentBlobKey>> blobKeys = uploadUserArtifacts(jobGraph.getJobID(), jobGraph.getUserArtifacts(), blobClient);
		setUserArtifactBlobKeys(jobGraph, blobKeys);
	}

	private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(JobID jobID, Map<String, DistributedCache.DistributedCacheEntry> userArtifacts, BlobClient blobClient) throws IOException {
		Collection<Tuple2<String, PermanentBlobKey>> blobKeys = new ArrayList<>(userArtifacts.size());
		for (Map.Entry<String, DistributedCache.DistributedCacheEntry> userArtifact : userArtifacts.entrySet()) {
			Path path = new Path(userArtifact.getValue().filePath);
			// only upload local files
			if (!path.getFileSystem().isDistributedFS()) {
				final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, new Path(userArtifact.getValue().filePath));
				blobKeys.add(Tuple2.of(userArtifact.getKey(), blobKey));
			}
		}
		return blobKeys;
	}

	private static void setUserArtifactBlobKeys(JobGraph jobGraph, Collection<Tuple2<String, PermanentBlobKey>> blobKeys) {
		blobKeys.forEach(blobKey -> jobGraph.setUserArtifactBlobKey(blobKey.f0, blobKey.f1));
		jobGraph.finalizeUserArtifactEntries();
	}
}
