/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.cache;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import eu.stratosphere.api.common.cache.DistributedCache.DistributedCacheEntry;

import eu.stratosphere.core.fs.local.LocalFileSystem;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Test delete process of {@link FileCache}. The local cache file should not be deleted why another task comes in 5 seconds.
 */
public class FileCacheDeleteValidationTest {
	FileCache fileCache = new FileCache();
	LocalFileSystem lfs = new LocalFileSystem();
	File f;


	String testFileContent = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
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

	@Before
	public void createTmpCacheFile() {
		f = new File(System.getProperty("java.io.tmpdir"), "cacheFile");
		try {
			Files.write(testFileContent, f, Charsets.UTF_8);
		} catch (IOException e) {
			throw new RuntimeException("Error initializing the test", e);
		}
	}

	@Test
	public void testFileReuseForNextTask() {
		JobID jobID = new JobID();
		String filePath = f.toURI().toString();
		fileCache.createTmpFile("test_file", new DistributedCacheEntry(filePath, false), jobID);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted error", e);
		}
		fileCache.deleteTmpFile("test_file", new DistributedCacheEntry(filePath, false), jobID);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted error", e);
		}
		//new task comes after 1 second
		try {
			Assert.assertTrue("Local cache file should not be deleted when another task comes in 5 seconds!", lfs.exists(fileCache.getTempDir(jobID, "cacheFile")));
		} catch (IOException e) {
			throw new RuntimeException("Interrupted error", e);
		}
		fileCache.createTmpFile("test_file", new DistributedCacheEntry(filePath, false), jobID);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted error", e);
		}
		fileCache.deleteTmpFile("test_file", new DistributedCacheEntry(filePath, false), jobID);
		try {
			Thread.sleep(7000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted error", e);
		}
		//no task comes in 7 seconds
		try {
			Assert.assertTrue("Local cache file should be deleted when no task comes in 5 seconds!", !lfs.exists(fileCache.getTempDir(jobID, "cacheFile")));
		} catch (IOException e) {
			throw new RuntimeException("Interrupted error", e);
		}
	}

	@After
	public void shutdown() {
		fileCache.shutdown();
	}
}
