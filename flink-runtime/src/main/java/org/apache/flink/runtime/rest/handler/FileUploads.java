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

import java.io.FileNotFoundException;
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
	private final Collection<Path> directoriesToClean;
	private final Collection<Path> uploadedFiles;

	@SuppressWarnings("resource")
	public static final FileUploads EMPTY = new FileUploads();

	public static FileUploads forDirectory(Path directory) throws IOException {
		final Collection<Path> files = new ArrayList<>(4);
		Preconditions.checkArgument(directory.isAbsolute(), "Path must be absolute.");
		Preconditions.checkArgument(Files.isDirectory(directory), "Path must be a directory.");

		FileAdderVisitor visitor = new FileAdderVisitor();
		Files.walkFileTree(directory, visitor);
		files.addAll(visitor.getContainedFiles());

		return new FileUploads(Collections.singleton(directory), files);
	}

	private FileUploads() {
		this.directoriesToClean = Collections.emptyList();
		this.uploadedFiles = Collections.emptyList();
	}

	public FileUploads(Collection<Path> directoriesToClean, Collection<Path> uploadedFiles) {
		this.directoriesToClean = Preconditions.checkNotNull(directoriesToClean);
		this.uploadedFiles = Preconditions.checkNotNull(uploadedFiles);
	}

	public Collection<Path> getUploadedFiles() {
		return uploadedFiles;
	}

	@Override
	public void close() throws IOException {
		for (Path file : uploadedFiles) {
			try {
				Files.delete(file);
			} catch (FileNotFoundException ignored) {
				// file may have been moved by a handler
			}
		}
		for (Path directory : directoriesToClean) {
			FileUtils.deleteDirectory(directory.toFile());
		}
	}

	private static final class FileAdderVisitor extends SimpleFileVisitor<Path> {

		private final Collection<Path> files = new ArrayList<>(4);

		Collection<Path> getContainedFiles() {
			return files;
		}

		FileAdderVisitor() {
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			FileVisitResult result = super.visitFile(file, attrs);
			files.add(file);
			return result;
		}
	}

}
