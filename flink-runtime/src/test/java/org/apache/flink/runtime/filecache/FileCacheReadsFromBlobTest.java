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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests that {@link FileCache} can read files from {@link BlobServer}. */
public class FileCacheReadsFromBlobTest {

    private static final String testFileContent =
            "Goethe - Faust: Der Tragoedie erster Teil\n"
                    + "Prolog im Himmel.\n"
                    + "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n"
                    + "Erzengel treten vor.\n"
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

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FileCache fileCache;

    private final PermanentBlobKey permanentBlobKey = new PermanentBlobKey();

    private final PermanentBlobService blobService =
            new PermanentBlobService() {
                @Override
                public File getFile(JobID jobId, PermanentBlobKey key) throws IOException {
                    if (key.equals(permanentBlobKey)) {
                        File f = temporaryFolder.newFile("cacheFile");
                        FileUtils.writeFileUtf8(f, testFileContent);
                        return f;
                    } else {
                        throw new IllegalArgumentException(
                                "This service contains only entry for " + permanentBlobKey);
                    }
                }

                @Override
                public void close() throws IOException {}
            };

    @Before
    public void setup() throws Exception {
        fileCache =
                new FileCache(
                        new String[] {temporaryFolder.newFolder().getAbsolutePath()}, blobService);
    }

    @After
    public void shutdown() {
        fileCache.shutdown();
    }

    @Test
    public void testFileDownloadedFromBlob() throws Exception {
        JobID jobID = new JobID();
        ExecutionAttemptID attemptID = new ExecutionAttemptID();

        final String fileName = "test_file";
        // copy / create the file
        final DistributedCache.DistributedCacheEntry entry =
                new DistributedCache.DistributedCacheEntry(
                        fileName, false, InstantiationUtil.serializeObject(permanentBlobKey));
        Future<Path> copyResult = fileCache.createTmpFile(fileName, entry, jobID, attemptID);

        final Path dstPath = copyResult.get();
        final String actualContent =
                Files.toString(new File(dstPath.toUri()), StandardCharsets.UTF_8);
        assertTrue(dstPath.getFileSystem().exists(dstPath));
        assertEquals(testFileContent, actualContent);
    }
}
