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

package org.apache.flink.fs.azurefs;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import com.azure.storage.file.datalake.models.PathItem;

import java.net.URI;

/** Utility methods for constructing Flink {@link Path} instances from DataLake SDK objects. */
final class AzurePathUtils {

    private AzurePathUtils() {}

    /**
     * Constructs a Flink {@link Path} from a DataLake {@link PathItem}.
     *
     * <p>The resulting path uses the scheme and authority from {@code filesystemUri} (e.g. {@code
     * abfss://container@account.dfs.core.windows.net}) combined with the item name from the
     * DataLake listing. The item name is stripped of leading and trailing whitespace and may be
     * specified with or without a leading slash; this method normalizes it by ensuring a single
     * leading slash before constructing the path.
     *
     * @param filesystemUri the base filesystem URI providing scheme and authority
     * @param pathItem the DataLake path item whose name becomes the path component
     * @return the fully qualified Flink path
     */
    static Path buildDataLakePath(final URI filesystemUri, final PathItem pathItem) {
        Preconditions.checkNotNull(filesystemUri, "filesystemUri must not be null");
        Preconditions.checkNotNull(pathItem, "pathItem must not be null");
        final String name = pathItem.getName().strip();
        final String absolutePath = name.startsWith("/") ? name : "/" + name;
        return new Path(filesystemUri.getScheme(), filesystemUri.getAuthority(), absolutePath);
    }
}
