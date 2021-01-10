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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.factories.TableFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * File system {@link TableFactory}.
 *
 * <p>1.The partition information should be in the file system path, whether it's a temporary table
 * or a catalog table. 2.Support insert into (append) and insert overwrite. 3.Support static and
 * dynamic partition inserting.
 */
public class FileSystemTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "filesystem";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        validate(helper);
        return new FileSystemTableSource(
                context,
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class),
                discoverFormatFactory(context));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        validate(helper);
        return new FileSystemTableSink(
                context,
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class),
                discoverFormatFactory(context),
                discoverEncodingFormat(context, BulkWriterFormatFactory.class),
                discoverEncodingFormat(context, SerializationFormatFactory.class));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileSystemOptions.PATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileSystemOptions.PARTITION_DEFAULT_NAME);
        options.add(FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE);
        options.add(FileSystemOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL);
        options.add(FileSystemOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL);
        options.add(FileSystemOptions.SINK_SHUFFLE_BY_PARTITION);
        options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND);
        options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS);
        options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
        options.add(FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER);
        options.add(FileSystemOptions.SINK_PARTITION_COMMIT_DELAY);
        options.add(FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
        options.add(FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_CLASS);
        options.add(FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        options.add(FileSystemOptions.AUTO_COMPACTION);
        options.add(FileSystemOptions.COMPACTION_FILE_SIZE);
        return options;
    }

    private void validate(FactoryUtil.TableFactoryHelper helper) {
        // Except format options, some formats like parquet and orc can not list all supported
        // options.
        helper.validateExcept(helper.getOptions().get(FactoryUtil.FORMAT) + ".");
    }

    private <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverDecodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverEncodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private FileSystemFormatFactory discoverFormatFactory(Context context) {
        if (formatFactoryExists(context, FileSystemFormatFactory.class)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            return FactoryUtil.discoverFactory(
                    Thread.currentThread().getContextClassLoader(),
                    FileSystemFormatFactory.class,
                    identifier);
        } else {
            return null;
        }
    }

    /**
     * Returns true if the format factory can be found using the given factory base class and
     * identifier.
     */
    private boolean formatFactoryExists(Context context, Class<?> factoryClass) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        String identifier = options.get(FactoryUtil.FORMAT);
        if (identifier == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FactoryUtil.FORMAT.key()));
        }

        final List<Factory> factories = new LinkedList<>();
        ServiceLoader.load(Factory.class, context.getClassLoader())
                .iterator()
                .forEachRemaining(factories::add);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(identifier))
                        .collect(Collectors.toList());

        return !matchingFactories.isEmpty();
    }
}
