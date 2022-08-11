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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveStatsUtil;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.fs.hdfs.HadoopFileSystem.toHadoopPath;

/**
 * Hive {@link TableMetaStoreFactory}, use {@link HiveMetastoreClientWrapper} to communicate with
 * hive meta store.
 */
public class HiveTableMetaStoreFactory implements TableMetaStoreFactory {

    private static final long serialVersionUID = 1L;

    private final JobConfWrapper conf;
    private final FileSystemFactory fileSystemFactory;
    private final String hiveVersion;
    private final String database;
    private final String tableName;
    private final boolean autoGatherStatistic;
    private final String successFileName;

    HiveTableMetaStoreFactory(
            JobConf conf,
            FileSystemFactory fileSystemFactory,
            String hiveVersion,
            String database,
            String tableName,
            boolean autoGatherStatistic,
            String successFileName) {
        this.conf = new JobConfWrapper(conf);
        this.fileSystemFactory = fileSystemFactory;
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
        this.autoGatherStatistic = autoGatherStatistic;
        this.successFileName = successFileName;
    }

    @Override
    public HiveTableMetaStore createTableMetaStore() throws Exception {
        return new HiveTableMetaStore();
    }

    private class HiveTableMetaStore implements TableMetaStore {

        private final HiveMetastoreClientWrapper client;
        private final StorageDescriptor sd;
        private final Table table;

        private HiveTableMetaStore() throws TException {
            client =
                    HiveMetastoreClientFactory.create(
                            HiveConfUtils.create(conf.conf()), hiveVersion);
            table = client.getTable(database, tableName);
            sd = table.getSd();
        }

        @Override
        public Path getLocationPath() {
            return new Path(sd.getLocation());
        }

        @Override
        public Optional<Path> getPartition(LinkedHashMap<String, String> partSpec)
                throws Exception {
            try {
                return Optional.of(
                        new Path(
                                client.getPartition(
                                                database,
                                                tableName,
                                                new ArrayList<>(partSpec.values()))
                                        .getSd()
                                        .getLocation()));
            } catch (NoSuchObjectException ignore) {
                return Optional.empty();
            }
        }

        @Override
        public void createOrAlterPartition(
                LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception {
            Partition partition;
            try {
                partition =
                        client.getPartition(
                                database, tableName, new ArrayList<>(partitionSpec.values()));
            } catch (NoSuchObjectException e) {
                createPartition(partitionSpec, partitionPath);
                return;
            }
            alterPartition(partitionSpec, partitionPath, partition);
        }

        @Override
        public void finishWritingTable(Path tablePath) throws Exception {
            if (autoGatherStatistic) {
                Map<String, String> newStats = gatherStats(tablePath, false);
                if (HiveStatsUtil.tableStatsChanged(newStats, table.getParameters())) {
                    table.getParameters().putAll(newStats);
                    client.alter_table(database, tableName, table);
                }
            }
        }

        private void createPartition(LinkedHashMap<String, String> partSpec, Path path)
                throws Exception {
            StorageDescriptor newSd = new StorageDescriptor(sd);
            newSd.setLocation(path.toString());
            Partition partition =
                    HiveTableUtil.createHivePartition(
                            database,
                            tableName,
                            new ArrayList<>(partSpec.values()),
                            newSd,
                            new HashMap<>());
            partition.setValues(new ArrayList<>(partSpec.values()));
            if (autoGatherStatistic) {
                partition.getParameters().putAll(gatherStats(path, false));
            }
            client.add_partition(partition);
        }

        private void alterPartition(
                LinkedHashMap<String, String> partitionSpec,
                Path partitionPath,
                Partition currentPartition)
                throws Exception {
            StorageDescriptor partSD = currentPartition.getSd();
            // the following logic copied from Hive::alterPartitionSpecInMemory
            partSD.setOutputFormat(sd.getOutputFormat());
            partSD.setInputFormat(sd.getInputFormat());
            partSD.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
            partSD.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
            partSD.setBucketCols(sd.getBucketCols());
            partSD.setNumBuckets(sd.getNumBuckets());
            partSD.setSortCols(sd.getSortCols());
            partSD.setLocation(partitionPath.toString());
            if (autoGatherStatistic) {
                currentPartition.getParameters().putAll(gatherStats(partitionPath, true));
            }
            client.alter_partition(database, tableName, currentPartition);
        }

        private Map<String, String> gatherStats(Path path, boolean isForAlterPartition)
                throws Exception {
            Map<String, String> statistic = new HashMap<>();
            InputFormat<?, ?> inputFormat =
                    ReflectionUtil.newInstance(getInputFormatClz(sd.getInputFormat()), conf.conf());
            long numRows = 0;
            long fileSize = 0;
            int numFiles = 0;
            long rawDataSize = 0;
            List<FileStatus> fileStatuses =
                    listDataFileRecursively(fileSystemFactory.create(path.toUri()), path);
            for (FileStatus file : fileStatuses) {
                InputSplit dummySplit =
                        new FileSplit(
                                toHadoopPath(file.getPath()),
                                0,
                                -1,
                                new String[] {sd.getLocation()});
                org.apache.hadoop.mapred.RecordReader<?, ?> recordReader =
                        inputFormat.getRecordReader(dummySplit, conf.conf(), Reporter.NULL);
                try {
                    if (recordReader instanceof StatsProvidingRecordReader) {
                        StatsProvidingRecordReader statsRR =
                                (StatsProvidingRecordReader) recordReader;
                        rawDataSize += statsRR.getStats().getRawDataSize();
                        numRows += statsRR.getStats().getRowCount();
                        fileSize += file.getLen();
                        numFiles += 1;
                    } else {
                        // if the reader initialized according to input format class isn't instance
                        // of StatsProvidingRecordReader, which means the store format is neither
                        // orc nor parquet. we only calculate totalSize and numFiles.

                        // but when it's for collect stats to alter partition, we can skip calculate
                        // totalSize and numFiles since it'll be calculated by Hive metastore
                        // forcibly while altering partition.
                        // so we return -1 directly to avoid gathering statistic in here since it's
                        // redundant and time-consuming
                        if (isForAlterPartition) {
                            statistic.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(-1));
                            statistic.put(StatsSetupConst.NUM_FILES, String.valueOf(-1));
                            return statistic;
                        } else {
                            fileSize =
                                    fileStatuses.stream()
                                            .map(FileStatus::getLen)
                                            .reduce(Long::sum)
                                            .orElse(0L);
                            statistic.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(fileSize));
                            statistic.put(
                                    StatsSetupConst.NUM_FILES, String.valueOf(fileStatuses.size()));
                        }
                        return statistic;
                    }
                } finally {
                    recordReader.close();
                }
            }
            CatalogTableStatistics tableStatistics =
                    new CatalogTableStatistics(numRows, numFiles, fileSize, rawDataSize);
            HiveStatsUtil.updateStats(tableStatistics, statistic);
            return statistic;
        }

        @Override
        public void close() {
            client.close();
        }

        private boolean isDataFile(FileStatus fileStatus) {
            String fileName = fileStatus.getPath().getName();
            return !fileName.startsWith(".") && !fileName.equals(successFileName);
        }

        private boolean isStagingDir(Path path) {
            // in batch mode, the name for staging dir starts with '.staging_', see
            // HiveTableSink#toStagingDir
            // in stream mode, the stage dir is the partition/table location, but the staging files
            // start with '.'
            return path.getPath().startsWith(".");
        }

        /** List data files recursively. */
        private List<FileStatus> listDataFileRecursively(FileSystem fileSystem, Path f)
                throws IOException {
            List<FileStatus> fileStatusList = new ArrayList<>();
            for (FileStatus fileStatus : fileSystem.listStatus(f)) {
                if (fileStatus.isDir() && !isStagingDir(fileStatus.getPath())) {
                    fileStatusList.addAll(
                            listDataFileRecursively(fileSystem, fileStatus.getPath()));
                } else {
                    if (isDataFile(fileStatus)) {
                        fileStatusList.add(fileStatus);
                    }
                }
            }
            return fileStatusList;
        }

        private Class<? extends InputFormat<?, ?>> getInputFormatClz(String inputFormatClz) {
            try {
                return (Class<? extends InputFormat<?, ?>>)
                        Class.forName(
                                inputFormatClz,
                                true,
                                Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new FlinkHiveException(
                        String.format(
                                "Unable to load the class of the input format %s.", inputFormatClz),
                        e);
            }
        }
    }
}
