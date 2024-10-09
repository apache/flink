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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Contains utility methods for clients. */
public enum ClientUtils {
    ;

    /**
     * Extracts all files required for the execution from the given {@link ExecutionPlan} and
     * uploads them using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param executionPlan executionPlan requiring files
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    public static void extractAndUploadExecutionPlanFiles(
            ExecutionPlan executionPlan,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        List<Path> userJars = executionPlan.getUserJars();
        Collection<Tuple2<String, Path>> userArtifacts =
                executionPlan.getUserArtifacts().entrySet().stream()
                        .map(
                                entry ->
                                        Tuple2.of(
                                                entry.getKey(),
                                                new Path(entry.getValue().filePath)))
                        .collect(Collectors.toList());

        uploadExecutionPlanFiles(executionPlan, userJars, userArtifacts, clientSupplier);
    }

    /**
     * Uploads the given jars and artifacts required for the execution of the given {@link
     * ExecutionPlan} using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param executionPlan executionPlan requiring files
     * @param userJars jars to upload
     * @param userArtifacts artifacts to upload
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    public static void uploadExecutionPlanFiles(
            ExecutionPlan executionPlan,
            Collection<Path> userJars,
            Collection<Tuple2<String, org.apache.flink.core.fs.Path>> userArtifacts,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        if (!userJars.isEmpty() || !userArtifacts.isEmpty()) {
            try (BlobClient client = clientSupplier.get()) {
                uploadAndSetUserJars(executionPlan, userJars, client);
                uploadAndSetUserArtifacts(executionPlan, userArtifacts, client);
            } catch (IOException ioe) {
                throw new FlinkException("Could not upload job files.", ioe);
            }
        }
        executionPlan.writeUserArtifactEntriesToConfiguration();
    }

    /**
     * Uploads the given user jars using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link ExecutionPlan}.
     *
     * @param executionPlan executionPlan requiring user jars
     * @param userJars jars to upload
     * @param blobClient client to upload jars with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserJars(
            ExecutionPlan executionPlan, Collection<Path> userJars, BlobClient blobClient)
            throws IOException {
        Collection<PermanentBlobKey> blobKeys =
                uploadUserJars(executionPlan.getJobID(), userJars, blobClient);
        setUserJarBlobKeys(blobKeys, executionPlan);
    }

    private static Collection<PermanentBlobKey> uploadUserJars(
            JobID jobId, Collection<Path> userJars, BlobClient blobClient) throws IOException {
        Collection<PermanentBlobKey> blobKeys = new ArrayList<>(userJars.size());
        for (Path jar : userJars) {
            final PermanentBlobKey blobKey = blobClient.uploadFile(jobId, jar);
            blobKeys.add(blobKey);
        }
        return blobKeys;
    }

    private static void setUserJarBlobKeys(
            Collection<PermanentBlobKey> blobKeys, ExecutionPlan executionPlan) {
        blobKeys.forEach(executionPlan::addUserJarBlobKey);
    }

    /**
     * Uploads the given user artifacts using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link ExecutionPlan}.
     *
     * @param executionPlan executionPlan requiring user artifacts
     * @param artifactPaths artifacts to upload
     * @param blobClient client to upload artifacts with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserArtifacts(
            ExecutionPlan executionPlan,
            Collection<Tuple2<String, Path>> artifactPaths,
            BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                uploadUserArtifacts(executionPlan.getJobID(), artifactPaths, blobClient);
        setUserArtifactBlobKeys(executionPlan, blobKeys);
    }

    private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(
            JobID jobID, Collection<Tuple2<String, Path>> userArtifacts, BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                new ArrayList<>(userArtifacts.size());
        for (Tuple2<String, Path> userArtifact : userArtifacts) {
            // only upload local files
            if (!userArtifact.f1.getFileSystem().isDistributedFS()) {
                final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, userArtifact.f1);
                blobKeys.add(Tuple2.of(userArtifact.f0, blobKey));
            }
        }
        return blobKeys;
    }

    private static void setUserArtifactBlobKeys(
            ExecutionPlan executionPlan, Collection<Tuple2<String, PermanentBlobKey>> blobKeys)
            throws IOException {
        for (Tuple2<String, PermanentBlobKey> blobKey : blobKeys) {
            executionPlan.setUserArtifactBlobKey(blobKey.f0, blobKey.f1);
        }
    }
}
