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

package org.apache.flink.runtime.filecache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that {@link FileCache} can read zipped directories from BlobServer and properly cleans them after.
 */
public class FileCacheDirectoriesTest {

	private static final String testFileContent = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
	+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
	+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
	+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
	+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
	+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
	+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
	+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
	+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
	+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
	+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
	+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
	+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
	+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.";

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private FileCache fileCache;

	private final PermanentBlobKey permanentBlobKey = new PermanentBlobKey();

	private final PermanentBlobService blobService = new PermanentBlobService() {
		@Override
		public File getFile(JobID jobId, PermanentBlobKey key) throws IOException {
			if (key.equals(permanentBlobKey)) {
				final File zipArchive = temporaryFolder.newFile("zipArchive");
				try (ZipOutputStream zis = new ZipOutputStream(new FileOutputStream(zipArchive))) {

					final ZipEntry zipEntry = new ZipEntry("cacheFile");
					zis.putNextEntry(zipEntry);

					IOUtils.copyBytes(new ByteArrayInputStream(testFileContent.getBytes(StandardCharsets.UTF_8)), zis, false);
				}
				return zipArchive;
			} else {
				throw new IllegalArgumentException("This service contains only entry for " + permanentBlobKey);
			}
		}

		@Override
		public void close() throws IOException {

		}
	};

	private static final int CLEANUP_INTERVAL = 1000;

	private DeleteCapturingDirectScheduledExecutorService executorService = new DeleteCapturingDirectScheduledExecutorService();

	@Before
	public void setup() throws Exception {
		fileCache = new FileCache(new String[]{temporaryFolder.newFolder().getAbsolutePath()}, blobService, executorService, CLEANUP_INTERVAL);
	}

	@After
	public void shutdown() {
		fileCache.shutdown();

		if (executorService.lastDeleteProcess != null) {
			executorService.lastDeleteProcess.run();
		}
	}

	@Test
	public void testDirectoryDownloadedFromBlob() throws Exception {
		JobID jobID = new JobID();
		ExecutionAttemptID attemptID = new ExecutionAttemptID();

		final String fileName = "test_file";
		// copy / create the file
		final DistributedCache.DistributedCacheEntry entry = new DistributedCache.DistributedCacheEntry(
			fileName,
			false,
			InstantiationUtil.serializeObject(permanentBlobKey),
			true);
		Future<Path> copyResult = fileCache.createTmpFile(fileName, entry, jobID, attemptID);

		final Path dstPath = copyResult.get();
		final FileSystem fs = dstPath.getFileSystem();
		final FileStatus fileStatus = fs.getFileStatus(dstPath);
		assertTrue(fileStatus.isDir());

		final Path cacheFile = new Path(dstPath, "cacheFile");
		assertTrue(fs.exists(cacheFile));
		final String actualContent = FileUtils.readFileUtf8(new File(cacheFile.getPath()));
		assertEquals(testFileContent, actualContent);
	}

	@Test
	public void testDirectoryCleanUp() throws Exception {
		JobID jobID = new JobID();
		ExecutionAttemptID attemptID1 = new ExecutionAttemptID();
		ExecutionAttemptID attemptID2 = new ExecutionAttemptID();

		final String fileName = "test_file";
		// copy / create the file
		final DistributedCache.DistributedCacheEntry entry = new DistributedCache.DistributedCacheEntry(
			fileName,
			false,
			InstantiationUtil.serializeObject(permanentBlobKey),
			true);
		Future<Path> copyResult = fileCache.createTmpFile(fileName, entry, jobID, attemptID1);
		fileCache.createTmpFile(fileName, entry, jobID, attemptID2);

		final Path dstPath = copyResult.get();
		final FileSystem fs = dstPath.getFileSystem();
		final FileStatus fileStatus = fs.getFileStatus(dstPath);
		final Path cacheFile = new Path(dstPath, "cacheFile");
		assertTrue(fileStatus.isDir());
		assertTrue(fs.exists(cacheFile));

		fileCache.releaseJob(jobID, attemptID1);
		// still should be available
		assertTrue(fileStatus.isDir());
		assertTrue(fs.exists(cacheFile));

		fileCache.releaseJob(jobID, attemptID2);
		// still should be available, file will be deleted after cleanupInterval
		assertTrue(fileStatus.isDir());
		assertTrue(fs.exists(cacheFile));

		// after a while, the file should disappear
		assertEquals(CLEANUP_INTERVAL, executorService.lastDelayMillis);
		executorService.lastDeleteProcess.run();

		assertFalse(fs.exists(dstPath));
		assertFalse(fs.exists(cacheFile));
	}

	private final class DeleteCapturingDirectScheduledExecutorService extends DirectScheduledExecutorService {

		FileCache.DeleteProcess lastDeleteProcess;
		long lastDelayMillis;

		@Override
		public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {

			if (command instanceof FileCache.DeleteProcess) {
				assertNull("Multiple delete process registered", lastDeleteProcess);
				lastDeleteProcess = (FileCache.DeleteProcess) command;
				lastDelayMillis = unit.toMillis(delay);
				return super.schedule(() -> {}, delay, unit);
			} else {
				return super.schedule(command, delay, unit);
			}
		}
	}
}
