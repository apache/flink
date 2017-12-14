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
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test delete process of {@link FileCache}. The local cache file should not be deleted why another task comes in 5 seconds.
 */
public class FileCacheDeleteValidationTest {

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
		+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.\n";

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private FileCache fileCache;
	private File f;

	@Before
	public void setup() throws IOException {
		String[] tmpDirectories = new String[]{temporaryFolder.newFolder().getAbsolutePath()};
		try {
			fileCache = new FileCache(tmpDirectories);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cannot create FileCache: " + e.getMessage());
		}

		f = temporaryFolder.newFile("cacheFile");
		try {
			Files.write(testFileContent, f, Charsets.UTF_8);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error initializing the test: " + e.getMessage());
		}
	}

	@After
	public void shutdown() {
		try {
			fileCache.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("FileCache shutdown failed: " + e.getMessage());
		}
	}

	@Test
	public void testFileReuseForNextTask() {
		try {
			final JobID jobID = new JobID();
			final String fileName = "test_file";

			final String filePath = f.toURI().toString();

			// copy / create the file
			Future<Path> copyResult = fileCache.createTmpFile(fileName, new DistributedCacheEntry(filePath, false), jobID);
			copyResult.get();

			// get another reference to the file
			Future<Path> copyResult2 = fileCache.createTmpFile(fileName, new DistributedCacheEntry(filePath, false), jobID);

			// this should be available immediately
			assertTrue(copyResult2.isDone());

			// delete the file
			fileCache.deleteTmpFile(fileName, jobID);
			// file should not yet be deleted
			assertTrue(fileCache.holdsStillReference(fileName, jobID));

			// delete the second reference
			fileCache.deleteTmpFile(fileName, jobID);
			// file should still not be deleted, but remain for a bit
			assertTrue(fileCache.holdsStillReference(fileName, jobID));

			fileCache.createTmpFile(fileName, new DistributedCacheEntry(filePath, false), jobID);
			fileCache.deleteTmpFile(fileName, jobID);

			// after a while, the file should disappear
			long deadline = System.currentTimeMillis() + 20000;
			do {
				Thread.sleep(5500);
			}
			while (fileCache.holdsStillReference(fileName, jobID) && System.currentTimeMillis() < deadline);

			assertFalse(fileCache.holdsStillReference(fileName, jobID));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
