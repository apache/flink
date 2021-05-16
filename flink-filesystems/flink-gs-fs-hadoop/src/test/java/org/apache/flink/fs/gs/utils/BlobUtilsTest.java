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

import org.apache.flink.fs.gs.storage.GSBlobIdentifier;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/** Test blob utilities. */
public class BlobUtilsTest {

    @Test
    public void shouldParseValidUri() {
        GSBlobIdentifier blobIdentifier = BlobUtils.parseUri(URI.create("gs://bucket/foo/bar"));
        assertEquals("bucket", blobIdentifier.bucketName);
        assertEquals("foo/bar", blobIdentifier.objectName);
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
