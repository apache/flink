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

package org.apache.flink.connector.file.src.compression;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.compression.Bzip2InputStreamFactory;
import org.apache.flink.api.common.io.compression.DeflateInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.GzipInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.XZInputStreamFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A collection of common compression formats and de-compressors. */
@PublicEvolving
public final class StandardDeCompressors {

    /** All supported file compression formats, by common file extensions. */
    private static final Map<String, InflaterInputStreamFactory<?>> DECOMPRESSORS =
            buildDecompressorMap(
                    DeflateInflaterInputStreamFactory.getInstance(),
                    GzipInflaterInputStreamFactory.getInstance(),
                    Bzip2InputStreamFactory.getInstance(),
                    XZInputStreamFactory.getInstance());

    /** All common file extensions of supported file compression formats. */
    private static final Collection<String> COMMON_SUFFIXES =
            Collections.unmodifiableList(new ArrayList<>(DECOMPRESSORS.keySet()));

    // ------------------------------------------------------------------------

    /** Gets all common file extensions of supported file compression formats. */
    public static Collection<String> getCommonSuffixes() {
        return COMMON_SUFFIXES;
    }

    /**
     * Gets the decompressor for a file extension. Returns null if there is no decompressor for this
     * file extension.
     */
    @Nullable
    public static InflaterInputStreamFactory<?> getDecompressorForExtension(String extension) {
        return DECOMPRESSORS.get(extension);
    }

    /**
     * Gets the decompressor for a file name. This checks the file against all known and supported
     * file extensions. Returns null if there is no decompressor for this file name.
     */
    @Nullable
    public static InflaterInputStreamFactory<?> getDecompressorForFileName(String fileName) {
        for (final Map.Entry<String, InflaterInputStreamFactory<?>> entry :
                DECOMPRESSORS.entrySet()) {
            if (fileName.endsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    // ------------------------------------------------------------------------

    private static Map<String, InflaterInputStreamFactory<?>> buildDecompressorMap(
            final InflaterInputStreamFactory<?>... decompressors) {

        final LinkedHashMap<String, InflaterInputStreamFactory<?>> map =
                new LinkedHashMap<>(decompressors.length);
        for (InflaterInputStreamFactory<?> decompressor : decompressors) {
            for (String suffix : decompressor.getCommonFileExtensions()) {
                map.put(suffix, decompressor);
            }
        }
        return map;
    }

    // ------------------------------------------------------------------------

    /** This class has purely static utility methods and is not meant to be instantiated. */
    private StandardDeCompressors() {}
}
