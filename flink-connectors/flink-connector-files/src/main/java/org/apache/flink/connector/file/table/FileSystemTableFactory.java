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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.factories.TableFactory;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * File system {@link TableFactory}.
 *
 * <p>1.The partition information should be in the file system path, whether it's a temporary table
 * or a catalog table. 2.Support insert into (append) and insert overwrite. 3.Support static and
 * dynamic partition inserting.
 */
@Internal
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
                context.getObjectIdentifier(),
                context.getPhysicalRowDataType(),
                context.getCatalogTable().getPartitionKeys(),
                helper.getOptions(),
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        validate(helper);
        return new FileSystemTableSink(
                context.getObjectIdentifier(),
                context.getPhysicalRowDataType(),
                context.getCatalogTable().getPartitionKeys(),
                helper.getOptions(),
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class),
                discoverEncodingFormat(context, BulkWriterFormatFactory.class),
                discoverEncodingFormat(context, SerializationFormatFactory.class));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileSystemConnectorOptions.PATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileSystemConnectorOptions.PARTITION_DEFAULT_NAME);
        options.add(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL);
        options.add(FileSystemConnectorOptions.SOURCE_REPORT_STATISTICS);
        options.add(FileSystemConnectorOptions.SOURCE_PATH_REGEX_PATTERN);
        options.add(FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE);
        options.add(FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL);
        options.add(FileSystemConnectorOptions.SINK_ROLLING_POLICY_INACTIVITY_INTERVAL);
        options.add(FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL);
        options.add(FileSystemConnectorOptions.SINK_SHUFFLE_BY_PARTITION);
        options.add(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND);
        options.add(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_CLASS);
        options.add(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER);
        options.add(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS);
        options.add(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        options.add(FileSystemConnectorOptions.AUTO_COMPACTION);
        options.add(FileSystemConnectorOptions.COMPACTION_FILE_SIZE);
        options.add(FileSystemConnectorOptions.SINK_PARALLELISM);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        FileSystemConnectorOptions.PATH,
                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME,
                        FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE,
                        FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL,
                        FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME,
                        FileSystemConnectorOptions.COMPACTION_FILE_SIZE)
                .collect(Collectors.toSet());
    }

    private void validate(FactoryUtil.TableFactoryHelper helper) {
        // Except format options, some formats like parquet and orc can not list all supported
        // options.
        helper.validateExcept(helper.getOptions().get(FactoryUtil.FORMAT) + ".");

        // validate time zone of watermark
        validateTimeZone(
                helper.getOptions()
                        .get(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE));
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

    /** Similar logic as for {@link TableConfig}. */
    private void validateTimeZone(String zone) {
        boolean isValid;
        try {
            // We enforce a zone string that is compatible with both java.util.TimeZone and
            // java.time.ZoneId to avoid bugs.
            // In general, advertising either TZDB ID, GMT+xx:xx, or UTC is the best we can do.
            isValid = java.util.TimeZone.getTimeZone(zone).toZoneId().equals(ZoneId.of(zone));
        } catch (Exception e) {
            isValid = false;
        }

        if (!isValid) {
            throw new ValidationException(
                    String.format(
                            "Invalid time zone for '%s'. The value should be a Time Zone Database (TZDB) ID "
                                    + "such as 'America/Los_Angeles' to include daylight saving time. Fixed "
                                    + "offsets are supported using 'GMT-03:00' or 'GMT+03:00'. Or use 'UTC' "
                                    + "without time zone and daylight saving time.",
                            FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE
                                    .key()));
        }
    }
}
