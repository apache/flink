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
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Contains utility methods for clients.
 */
public enum ClientUtils {
	;

	/**
	 * Uploads all files required for the execution of the given {@link JobGraph} using the {@link BlobClient} from
	 * the given {@link Supplier}.
	 *
	 * @param jobGraph jobgraph requiring files
	 * @param clientSupplier supplier of blob client to upload files with
	 * @throws IOException if the upload fails
	 */
	public static void uploadJobGraphFiles(JobGraph jobGraph, SupplierWithException<BlobClient, IOException> clientSupplier) throws FlinkException {
		List<Path> userJars = jobGraph.getUserJars();
		Map<String, DistributedCache.DistributedCacheEntry> userArtifacts = jobGraph.getUserArtifacts();
		if (!userJars.isEmpty() || !userArtifacts.isEmpty()) {
			try (BlobClient client = clientSupplier.get()) {
				uploadAndSetUserJars(jobGraph, client);
				uploadAndSetUserArtifacts(jobGraph, client);
			} catch (IOException ioe) {
				throw new FlinkException("Could not upload job files.", ioe);
			}
		}
	}

	/**
	 * Uploads the user jars from the given {@link JobGraph} using the given {@link BlobClient},
	 * and sets the appropriate blobkeys.
	 *
	 * @param jobGraph   jobgraph requiring user jars
	 * @param blobClient client to upload jars with
	 * @throws IOException if the upload fails
	 */
	private static void uploadAndSetUserJars(JobGraph jobGraph, BlobClient blobClient) throws IOException {
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
	private static void uploadAndSetUserArtifacts(JobGraph jobGraph, BlobClient blobClient) throws IOException {
		Collection<Tuple2<String, Path>> artifactPaths = jobGraph.getUserArtifacts().entrySet().stream()
			.map(entry -> Tuple2.of(entry.getKey(), new Path(entry.getValue().filePath)))
			.collect(Collectors.toList());

		Collection<Tuple2<String, PermanentBlobKey>> blobKeys = uploadUserArtifacts(jobGraph.getJobID(), artifactPaths, blobClient);
		setUserArtifactBlobKeys(jobGraph, blobKeys);
	}

	private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(JobID jobID, Collection<Tuple2<String, Path>> userArtifacts, BlobClient blobClient) throws IOException {
		Collection<Tuple2<String, PermanentBlobKey>> blobKeys = new ArrayList<>(userArtifacts.size());
		for (Tuple2<String, Path> userArtifact : userArtifacts) {
			// only upload local files
			if (!userArtifact.f1.getFileSystem().isDistributedFS()) {
				final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, userArtifact.f1);
				blobKeys.add(Tuple2.of(userArtifact.f0, blobKey));
			}
		}
		return blobKeys;
	}

	private static void setUserArtifactBlobKeys(JobGraph jobGraph, Collection<Tuple2<String, PermanentBlobKey>> blobKeys) throws IOException {
		for (Tuple2<String, PermanentBlobKey> blobKey : blobKeys) {
			jobGraph.setUserArtifactBlobKey(blobKey.f0, blobKey.f1);
		}
		jobGraph.writeUserArtifactEntriesToConfiguration();
	}
}
