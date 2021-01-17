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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.operators.testutils.ExpectedTestException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

/** Tests for {@link RocksDBStateBackend} on initialization. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RocksDB.class})
public class RocksDBInitTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * This test checks that the RocksDB native code loader still responds to resetting the init
     * flag.
     */
    @Test
    public void testResetInitFlag() throws Exception {
        RocksDBStateBackend.resetRocksDBLoadedFlag();
    }

    @Test
    public void testTempLibFolderDeletedOnFail() throws Exception {
        PowerMockito.spy(RocksDB.class);
        PowerMockito.when(RocksDB.class, "loadLibrary").thenThrow(new ExpectedTestException());

        File tempFolder = temporaryFolder.newFolder();
        try {
            RocksDBStateBackend.ensureRocksDBIsLoaded(tempFolder.getAbsolutePath());
            fail("Not throwing expected exception.");
        } catch (IOException ignored) {
            // ignored
        }
        File[] files = tempFolder.listFiles();
        Assert.assertNotNull(files);
        Assert.assertEquals(0, files.length);
    }
}
