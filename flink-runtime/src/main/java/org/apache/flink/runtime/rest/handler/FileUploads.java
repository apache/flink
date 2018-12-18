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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A container for uploaded files.
 *
 * <p>Implementation note: The constructor also accepts directories to ensure that the upload directories are cleaned up.
 * For convenience during testing it also accepts files directly.
 */
public final class FileUploads implements AutoCloseable {
	@Nullable
	private final Path uploadDirectory;

	public static final FileUploads EMPTY = new FileUploads();

	private FileUploads() {
		this.uploadDirectory = null;
	}

	public FileUploads(@Nonnull Path uploadDirectory) {
		Preconditions.checkNotNull(uploadDirectory, "UploadDirectory must not be null.");
		Preconditions.checkArgument(Files.exists(uploadDirectory), "UploadDirectory does not exist.");
		Preconditions.checkArgument(Files.isDirectory(uploadDirectory), "UploadDirectory is not a directory.");
		Preconditions.checkArgument(uploadDirectory.isAbsolute(), "UploadDirectory is not absolute.");
		this.uploadDirectory = uploadDirectory;
	}

	public Collection<File> getUploadedFiles() throws IOException {
		if (uploadDirectory == null) {
			return Collections.emptyList();
		}

		FileAdderVisitor visitor = new FileAdderVisitor();
		Files.walkFileTree(uploadDirectory, visitor);

		return Collections.unmodifiableCollection(visitor.getContainedFiles());
	}

	@Override
	public void close() throws IOException {
		if (uploadDirectory != null) {
			FileUtils.deleteDirectory(uploadDirectory.toFile());
		}
	}

	private static final class FileAdderVisitor extends SimpleFileVisitor<Path> {

		private final Collection<File> files = new ArrayList<>(4);

		Collection<File> getContainedFiles() {
			return files;
		}

		FileAdderVisitor() {
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			FileVisitResult result = super.visitFile(file, attrs);
			files.add(file.toFile());
			return result;
		}
	}

}
