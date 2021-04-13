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

package org.apache.flink.fs.gs.utils;

import com.google.cloud.storage.BlobId;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Test blob utilities. */
public class BlobUtilsTest {

    @Test
    public void shouldNormalizeBlobId() {
        BlobId normalizedBlobId = BlobUtils.normalizeBlobId(BlobId.of("bucket", "foo/bar", 12L));
        assertEquals("bucket", normalizedBlobId.getBucket());
        assertEquals("foo/bar", normalizedBlobId.getName());
        assertNull(normalizedBlobId.getGeneration());
    }

    @Test
    public void shouldParseValidUri() {
        BlobId blobId = BlobUtils.parseUri(URI.create("gs://bucket/foo/bar"));
        assertEquals("bucket", blobId.getBucket());
        assertEquals("foo/bar", blobId.getName());
        assertNull(blobId.getGeneration());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToParseUriMissingBucketName() {
        BlobUtils.parseUri(URI.create("gs:///foo/bar"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToParseUriMissingObjectName() {
        BlobUtils.parseUri(URI.create("gs://bucket/"));
    }
}
