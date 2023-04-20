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

package org.apache.flink.fs.s3presto;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

/**
 * {@code FlinkS3PrestoFileSystem} provides custom recursive deletion functionality to work around a
 * bug in the internally used Presto file system.
 *
 * <p>https://github.com/prestodb/presto/issues/17416
 */
public class FlinkS3PrestoFileSystem extends FlinkS3FileSystem {

    public FlinkS3PrestoFileSystem(
            FileSystem hadoopS3FileSystem,
            String localTmpDirectory,
            @Nullable String entropyInjectionKey,
            int entropyLength,
            @Nullable S3AccessHelper s3UploadHelper,
            long s3uploadPartSize,
            int maxConcurrentUploadsPerStream) {
        super(
                hadoopS3FileSystem,
                localTmpDirectory,
                entropyInjectionKey,
                entropyLength,
                s3UploadHelper,
                s3uploadPartSize,
                maxConcurrentUploadsPerStream);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        if (recursive) {
            deleteRecursively(path);
        } else {
            deleteObject(path);
        }

        return true;
    }

    private void deleteRecursively(Path path) throws IOException {
        final FileStatus[] containingFiles =
                Preconditions.checkNotNull(
                        listStatus(path),
                        "Hadoop FileSystem.listStatus should never return null based on its contract.");

        if (containingFiles.length == 0) {
            // This if branch covers objects and empty directories. Both will be handled properly in
            // the deleteObject method.
            deleteObject(path);
            return;
        }

        IOException exception = null;
        for (FileStatus fileStatus : containingFiles) {
            final Path childPath = fileStatus.getPath();

            try {
                if (fileStatus.isDir()) {
                    deleteRecursively(childPath);
                } else {
                    deleteObject(childPath);
                }
            } catch (IOException e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        // Presto doesn't hold placeholders for directories itself; therefore, we don't need to
        // call deleteObject on the directory itself if there were objects with the prefix being
        // deleted in the initial loop above. This saves us from doing some existence checks on
        // an not-existing object (i.e. the now empty directory)
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Deletes the object referenced by the passed {@code path}. This method is used to work around
     * the fact that Presto doesn't allow us to differentiate between deleting a non-existing object
     * and some other errors. Therefore, a final check for existence is necessary in case of an
     * error or false return value.
     *
     * @param path The path referring to the object that shall be deleted.
     * @throws IOException if an error occurred while deleting the file other than the {@code path}
     *     referring to a non-empty directory.
     */
    private void deleteObject(Path path) throws IOException {
        boolean success = true;
        IOException actualException = null;
        try {
            // empty directories will cause this method to fail as well - checking for their
            // existence afterwards is a workaround to cover this use-case
            success = super.delete(path, false);
        } catch (IOException e) {
            actualException = e;
        }

        if (!success || actualException != null) {
            if (exists(path)) {
                throw Optional.ofNullable(actualException)
                        .orElse(
                                new IOException(
                                        path.getPath()
                                                + " could not be deleted for unknown reasons."));
            }
        }
    }
}
