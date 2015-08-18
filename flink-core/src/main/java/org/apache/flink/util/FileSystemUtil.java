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

/* This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.flink.util;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import java.net.URI;

public class FileSystemUtil {

    /**
     * Return a qualified path object
     * @param path
     *        the path will be qualified
     * @param fs
     *        the FileSystem that should be used to obtain the current working directory
     */

    public static Path makeQualified(Path path , FileSystem fs) {
        if (!path.isAbsolute()) {
            path = new Path(fs.getWorkingDirectory() , path);
        }

        final URI pathUri = path.toUri();
        final URI fsUri = fs.getUri();

        String scheme = pathUri.getScheme();
        String authority = pathUri.getAuthority();
        String fsAuthority = fsUri.getAuthority();

        if (scheme != null && (authority != null || fsAuthority == null)) {
            return path;
        }

        if (scheme == null) {
            scheme = fsUri.getScheme();
        }

        if (authority == null) {
            authority = (fsAuthority == null) ?"":fsAuthority;
        }

        return new Path(scheme + ":" + "//" + authority + pathUri.getPath());
    }
}