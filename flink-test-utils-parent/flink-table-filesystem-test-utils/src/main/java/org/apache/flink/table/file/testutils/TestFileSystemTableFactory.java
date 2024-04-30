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

package org.apache.flink.table.file.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.connector.file.table.TestFileSystemTableSource;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.file.testutils.catalog.TestFileSystemCatalog;

import java.util.Collections;

/** Test filesystem {@link Factory}. */
@Internal
public class TestFileSystemTableFactory extends FileSystemTableFactory {

    public static final String IDENTIFIER = "test-filesystem";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final boolean isFileSystemTable =
                TestFileSystemCatalog.isFileSystemTable(context.getCatalogTable().getOptions());
        if (!isFileSystemTable) {
            return FactoryUtil.createDynamicTableSource(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    Collections.emptyMap(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        validate(helper);
        return new TestFileSystemTableSource(
                context.getObjectIdentifier(),
                context.getPhysicalRowDataType(),
                context.getCatalogTable().getPartitionKeys(),
                helper.getOptions(),
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final boolean isFileSystemTable =
                TestFileSystemCatalog.isFileSystemTable(context.getCatalogTable().getOptions());
        if (!isFileSystemTable) {
            return FactoryUtil.createDynamicTableSink(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    Collections.emptyMap(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
        return super.createDynamicTableSink(context);
    }
}
