/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Operates the output stream in two phrases, any exception during the operation of {@link TwoPhraseFSDataOutputStream} will
 * lead the {@link #targetFile} to be invisible.
 *
 * <p>PHRASE 1, write the data into the {@link #preparingFile}.
 * <p>PHRASE 2, close the {@link #preparingFile} and rename it to the {@link #targetFile}.
 */
@Internal
public class TwoPhraseFSDataOutputStream extends FSDataOutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(TwoPhraseFSDataOutputStream.class);

	private static final String PREPARING_FILE_SUFFIX = "_tmp";

	/**
	 * the target file system.
	 */
	private final FileSystem fs;

	/**
	 * the target file which the preparing file will be renamed to in the close().
	 */
	private final Path targetFile;

	/**
	 * the preparing file to store the on flying data.
	 */
	private final Path preparingFile;

	/**
	 * the output stream of the preparing file.
	 */
	private final FSDataOutputStream preparedOutputStream;

	enum PhraseType {PREPARE, COMMIT}

	private PhraseType currentPhrase;

	public TwoPhraseFSDataOutputStream(FileSystem fs, Path f, FileSystem.WriteMode writeMode) throws IOException {

		this.fs = fs;
		this.targetFile = f;
		this.preparingFile = new Path(f.getPath() + PREPARING_FILE_SUFFIX);
		this.currentPhrase = PhraseType.PREPARE;

		if (writeMode == FileSystem.WriteMode.NO_OVERWRITE && fs.exists(targetFile)) {
			throw new IOException("Target file " + targetFile + " is already exists.");
		}

		this.preparedOutputStream = fs.create(this.preparingFile, writeMode);
	}

	@Override
	public long getPos() throws IOException {
		return this.preparedOutputStream.getPos();
	}

	@Override
	public void write(int b) throws IOException {
		checkPhrase(PhraseType.PREPARE);
		this.preparedOutputStream.write(b);
	}

	@Override
	public void flush() throws IOException {
		checkPhrase(PhraseType.PREPARE);
		this.preparedOutputStream.flush();
	}

	@Override
	public void sync() throws IOException {
		checkPhrase(PhraseType.PREPARE);
		this.preparedOutputStream.sync();
	}

	@Override
	public void close() throws IOException {
		try {
			checkPhrase(PhraseType.COMMIT);
			this.preparedOutputStream.close();
			this.fs.rename(preparingFile, targetFile);
		} catch (Exception e) {
			this.preparedOutputStream.close();
			if (fs.exists(preparingFile)) {
				try {
					fs.delete(preparingFile, false);
				} catch (Throwable ignored) {
					LOG.warn("Failed to delete the preparing file {" + preparingFile + "}.", ignored);
				}
			}
			throw e;
		}
	}

	private void checkPhrase(PhraseType phrase) {
		if (currentPhrase != phrase) {
			throw new IllegalStateException("we are not in " + phrase + " phrase currently.");
		}
	}

	public void commit() {
		currentPhrase = PhraseType.COMMIT;
	}

	@VisibleForTesting
	public Path getPreparingFile() {
		return preparingFile;
	}
}
