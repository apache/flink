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

package org.apache.flink.runtime.rest.handler.util;

import java.util.HashMap;

/**
 * Simple utility class that resolves file extensions to MIME types.
 *
 * <p>There are various solutions built into Java that depend on extra resource and configuration
 * files. They are designed to be composable and extensible, but also unfortunately tricky to
 * control. This is meant to be a simple solution that may eventually be subsumed by a better one.
 */
public class MimeTypes {

    /** The default mime type. */
    private static final String DEFAULT_MIME_TYPE = "application/octet-stream";

    /** The map with the constants. */
    private static final HashMap<String, String> MIME_MAP = new HashMap<String, String>();

    /**
     * Gets the MIME type for the file with the given extension. If the mime type is not recognized,
     * this method returns null.
     *
     * @param fileExtension The file extension.
     * @return The MIME type, or {@code null}, if the file extension is not recognized.
     */
    public static String getMimeTypeForExtension(String fileExtension) {
        return MIME_MAP.get(fileExtension.toLowerCase());
    }

    /**
     * Gets the MIME type for the file with the given name, by extension. This method tries to
     * extract the file extension and then use the {@link #getMimeTypeForExtension(String)} to
     * determine the MIME type. If the extension cannot be determined, or the extension is
     * unrecognized, this method return {@code null}.
     *
     * @param fileName The file name.
     * @return The MIME type, or {@code null}, if the file's extension is not recognized.
     */
    public static String getMimeTypeForFileName(String fileName) {
        int extensionPos = fileName.lastIndexOf('.');
        if (extensionPos >= 1 && extensionPos < fileName.length() - 1) {
            String extension = fileName.substring(extensionPos + 1);
            return getMimeTypeForExtension(extension);
        } else {
            return null;
        }
    }

    /**
     * Gets the default MIME type, which is {@code "application/octet-stream"}.
     *
     * @return The default MIME type.
     */
    public static String getDefaultMimeType() {
        return DEFAULT_MIME_TYPE;
    }

    // ------------------------------------------------------------------------
    //  prevent instantiation
    // ------------------------------------------------------------------------

    private MimeTypes() {}

    // ------------------------------------------------------------------------
    //  initialization
    // ------------------------------------------------------------------------

    static {
        // text types
        MIME_MAP.put("html", "text/html");
        MIME_MAP.put("htm", "text/html");
        MIME_MAP.put("css", "text/css");
        MIME_MAP.put("txt", "text/plain");
        MIME_MAP.put("log", "text/plain");
        MIME_MAP.put("out", "text/plain");
        MIME_MAP.put("err", "text/plain");
        MIME_MAP.put("xml", "text/xml");
        MIME_MAP.put("csv", "text/csv");

        // application types
        MIME_MAP.put("js", "application/javascript");
        MIME_MAP.put("json", "application/json");

        // image types
        MIME_MAP.put("png", "image/png");
        MIME_MAP.put("jpg", "image/jpeg");
        MIME_MAP.put("jpeg", "image/jpeg");
        MIME_MAP.put("gif", "image/gif");
        MIME_MAP.put("svg", "image/svg+xml");
        MIME_MAP.put("tiff", "image/tiff");
        MIME_MAP.put("tff", "image/tiff");
        MIME_MAP.put("bmp", "image/bmp");

        // fonts
        MIME_MAP.put("woff", "application/font-woff");
        MIME_MAP.put("woff2", "application/font-woff2");
        MIME_MAP.put("ttf", "font/ttf");
        MIME_MAP.put("otf", "font/opentype");
        MIME_MAP.put("eot", "font/application/vnd.ms-fontobject");
    }
}
