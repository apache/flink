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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/** An abstract container overlay. */
abstract class AbstractContainerOverlay implements ContainerOverlay {

    /**
     * Add a path recursively to the container specification.
     *
     * <p>If the path is a directory, the directory itself (not just its contents) is added to the
     * target path.
     *
     * <p>The execute bit is preserved; permissions aren't.
     *
     * @param sourcePath the path to add.
     * @param targetPath the target path.
     * @param env the specification to mutate.
     * @throws IOException
     */
    protected void addPathRecursively(
            final File sourcePath, final Path targetPath, final ContainerSpecification env)
            throws IOException {

        final java.nio.file.Path sourceRoot = sourcePath.toPath().getParent();

        Files.walkFileTree(
                sourcePath.toPath(),
                new SimpleFileVisitor<java.nio.file.Path>() {
                    @Override
                    public FileVisitResult visitFile(
                            java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {

                        java.nio.file.Path relativePath = sourceRoot.relativize(file);

                        ContainerSpecification.Artifact.Builder artifact =
                                ContainerSpecification.Artifact.newBuilder()
                                        .setSource(new Path(file.toUri()))
                                        .setDest(new Path(targetPath, relativePath.toString()))
                                        .setExecutable(Files.isExecutable(file))
                                        .setCachable(true)
                                        .setExtract(false);

                        env.getArtifacts().add(artifact.build());

                        return super.visitFile(file, attrs);
                    }
                });
    }
}
