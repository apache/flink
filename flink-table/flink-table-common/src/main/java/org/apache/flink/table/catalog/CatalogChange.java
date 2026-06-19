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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.util.function.Consumer;

/** {@link CatalogChange} represents the modification of the catalog. */
@Internal
public interface CatalogChange {

    /** Generate a new CatalogDescriptor after applying the change to the given descriptor. */
    CatalogDescriptor applyChange(CatalogDescriptor descriptor);

    // --------------------------------------------------------------------------------------------
    // Option Change
    // --------------------------------------------------------------------------------------------

    /** A catalog change to modify the catalog configuration. */
    @Internal
    class CatalogConfigurationChange implements CatalogChange {

        private final Consumer<Configuration> configUpdater;

        public CatalogConfigurationChange(Consumer<Configuration> configUpdater) {
            this.configUpdater = configUpdater;
        }

        @Override
        public CatalogDescriptor applyChange(CatalogDescriptor descriptor) {
            Configuration conf = descriptor.getConfiguration();
            configUpdater.accept(conf);
            return CatalogDescriptor.of(
                    descriptor.getCatalogName(), conf, descriptor.getComment().orElse(null));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Comment Change
    // --------------------------------------------------------------------------------------------

    /** A catalog change to modify the comment. */
    @Internal
    class CatalogCommentChange implements CatalogChange {

        private final String newComment;

        public CatalogCommentChange(String newComment) {
            this.newComment = newComment;
        }

        @Override
        public CatalogDescriptor applyChange(CatalogDescriptor descriptor) {
            return descriptor.setComment(newComment);
        }
    }
}
