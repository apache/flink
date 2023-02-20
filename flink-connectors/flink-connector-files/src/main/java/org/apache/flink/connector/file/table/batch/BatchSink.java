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

package org.apache.flink.connector.file.table.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.FileSystemOutputFormat;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.batch.compact.BatchCompactCoordinator;
import org.apache.flink.connector.file.table.batch.compact.BatchCompactOperator;
import org.apache.flink.connector.file.table.batch.compact.BatchPartitionCommitterSink;
import org.apache.flink.connector.file.table.stream.compact.CompactBucketWriter;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;

/** Helper for creating batch file sink. */
@Internal
public class BatchSink {

    public static final String COORDINATOR_OP_NAME = "compact-coordinator";

    public static final String COMPACT_OP_NAME = "compact-operator";

    private BatchSink() {}

    public static DataStreamSink<Row> createBatchNoCompactSink(
            DataStream<RowData> dataStream,
            DynamicTableSink.DataStructureConverter converter,
            FileSystemOutputFormat<Row> fileSystemOutputFormat,
            final int parallelism,
            final boolean parallelismConfigured) {
        SingleOutputStreamOperator<Row> map =
                dataStream.map(value -> ((Row) converter.toExternal(value)));
        map.getTransformation().setParallelism(parallelism, parallelismConfigured);

        DataStreamSink<Row> sink = map.writeUsingOutputFormat(fileSystemOutputFormat);
        sink.getTransformation().setParallelism(parallelism, parallelismConfigured);
        return sink;
    }

    public static <T> DataStreamSink<?> createBatchCompactSink(
            DataStream<CoordinatorInput> dataStream,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    builder,
            CompactReader.Factory<T> readFactory,
            FileSystemFactory fsFactory,
            TableMetaStoreFactory metaStoreFactory,
            PartitionCommitPolicyFactory partitionCommitPolicyFactory,
            String[] partitionColumns,
            LinkedHashMap<String, String> staticPartitionSpec,
            Path tmpPath,
            ObjectIdentifier identifier,
            final long compactAverageSize,
            final long compactTargetSize,
            boolean isToLocal,
            boolean overwrite,
            final int compactParallelism,
            final boolean compactParallelismConfigured) {
        SupplierWithException<FileSystem, IOException> fsSupplier =
                (SupplierWithException<FileSystem, IOException> & Serializable)
                        () -> fsFactory.create(tmpPath.toUri());

        CompactWriter.Factory<T> writerFactory =
                CompactBucketWriter.factory(
                        (SupplierWithException<BucketWriter<T, String>, IOException> & Serializable)
                                builder::createBucketWriter);

        SingleOutputStreamOperator<CompactMessages.CompactOutput> transform =
                dataStream
                        .transform(
                                COORDINATOR_OP_NAME,
                                TypeInformation.of(CompactMessages.CoordinatorOutput.class),
                                new BatchCompactCoordinator(
                                        fsSupplier, compactAverageSize, compactTargetSize))
                        .setParallelism(1)
                        .setMaxParallelism(1)
                        .transform(
                                COMPACT_OP_NAME,
                                TypeInformation.of(CompactMessages.CompactOutput.class),
                                new BatchCompactOperator<>(fsSupplier, readFactory, writerFactory));
        transform
                .getTransformation()
                .setParallelism(compactParallelism, compactParallelismConfigured);

        return transform
                .addSink(
                        new BatchPartitionCommitterSink(
                                fsFactory,
                                metaStoreFactory,
                                overwrite,
                                isToLocal,
                                tmpPath,
                                partitionColumns,
                                staticPartitionSpec,
                                identifier,
                                partitionCommitPolicyFactory))
                .setParallelism(1);
    }
}
