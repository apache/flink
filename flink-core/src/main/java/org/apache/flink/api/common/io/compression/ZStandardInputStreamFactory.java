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

package org.apache.flink.api.common.io.compression;

import org.apache.flink.annotation.Internal;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

/** Factory for ZStandard decompressors. */
@Internal
public class ZStandardInputStreamFactory
        implements InflaterInputStreamFactory<ZstdCompressorInputStream> {

    private static final ZStandardInputStreamFactory INSTANCE = new ZStandardInputStreamFactory();

    public static ZStandardInputStreamFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public ZstdCompressorInputStream create(InputStream in) throws IOException {
        return new ZstdCompressorInputStream(in);
    }

    @Override
    public Collection<String> getCommonFileExtensions() {
        return Collections.singleton("zst");
    }
}
