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
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.fs.hdfs.HadoopFileSystem.toHadoopPath;
import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;

/**
 * Hive {@link TableMetaStoreFactory}, use {@link HiveMetastoreClientWrapper} to communicate with
 * hive meta store.
 */
public class HiveTableMetaStoreFactory implements TableMetaStoreFactory {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableMetaStoreFactory.class);

    private final JobConfWrapper conf;
    private final FileSystemFactory fileSystemFactory;
    private final String hiveVersion;
    private final String database;
    private final String tableName;
    private final boolean autoGatherStatistic;
    private final String successFileName;
    private final int gatherStatsThreadNum;
    private ExecutorService executorService;

    HiveTableMetaStoreFactory(
            JobConf conf,
            FileSystemFactory fileSystemFactory,
            String hiveVersion,
            String database,
            String tableName,
            String successFileName,
            boolean autoGatherStatistic,
            int gatherStatsThreadNum) {
        this.conf = new JobConfWrapper(conf);
        this.fileSystemFactory = fileSystemFactory;
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
        this.autoGatherStatistic = autoGatherStatistic;
        this.gatherStatsThreadNum = gatherStatsThreadNum;
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
            FileSystem fileSystem = fileSystemFactory.create(path.toUri());
            Optional<Map<String, String>> stats = gatherFullStats(fileSystem, path);
            if (stats.isPresent()) {
                return stats.get();
            } else {
                // now, we only gather fileSize and numFiles.
                // but if it's for collect stats to alter partition, we can skip
                // calculate totalSize and numFiles since it'll be calculated by Hive metastore
                // forcibly while altering partition. so we return -1 directly to avoid gathering
                // statistic in here since it's redundant and time-consuming
                long fileSize = 0;
                int numFiles = 0;
                if (isForAlterPartition) {
                    statistic.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(-1));
                    statistic.put(StatsSetupConst.NUM_FILES, String.valueOf(-1));
                } else {
                    for (FileStatus fileStatus :
                            listDataFileRecursively(fileSystemFactory.create(path.toUri()), path)) {
                        numFiles += 1;
                        fileSize += fileStatus.getLen();
                    }
                    statistic.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(fileSize));
                    statistic.put(StatsSetupConst.NUM_FILES, String.valueOf(numFiles));
                }
                return statistic;
            }
        }

        @Override
        public void close() {
            client.close();
            if (executorService != null) {
                executorService.shutdown();
            }
        }

        private Optional<Map<String, String>> gatherFullStats(FileSystem fileSystem, Path path)
                throws Exception {
            Map<String, String> statistic = new HashMap<>();
            if (!fileSystem.exists(path)) {
                // will happen when insert into a static partition without data, each stat is zero
                HiveStatsUtil.updateStats(new CatalogTableStatistics(0, 0, 0, 0), statistic);
                return Optional.of(statistic);
            }
            InputFormat<?, ?> inputFormat =
                    ReflectionUtil.newInstance(getInputFormatClz(sd.getInputFormat()), conf.conf());
            if (inputFormat instanceof OrcInputFormat
                    || inputFormat instanceof MapredParquetInputFormat) {
                List<Future<CatalogTableStatistics>> statsFutureList = new ArrayList<>();
                for (FileStatus fileStatus : listDataFileRecursively(fileSystem, path)) {
                    InputSplit dummySplit =
                            new FileSplit(
                                    toHadoopPath(fileStatus.getPath()),
                                    0,
                                    -1,
                                    new String[] {sd.getLocation()});
                    org.apache.hadoop.mapred.RecordReader<?, ?> recordReader =
                            inputFormat.getRecordReader(dummySplit, conf.conf(), Reporter.NULL);
                    if (recordReader instanceof StatsProvidingRecordReader) {
                        statsFutureList.add(
                                submitStatsGatherTask(
                                        new FileStatisticGather(
                                                fileStatus,
                                                (StatsProvidingRecordReader) recordReader)));
                    } else {
                        // won't fall into here theoretically if the inputFormat is instanceof
                        // OrcInputFormat or MapredParquetInputFormat, but the Hive's implementation
                        // may change which may cause falling into here.
                        LOG.warn(
                                "The inputFormat is instanceof OrcInputFormat or MapredParquetInputFormat,"
                                        + " but the RecordReader from the inputFormat is not instance of StatsProvidingRecordReader."
                                        + " So the statistic numRows/rawDataSize can't be gathered");
                        statsFutureList.forEach(
                                catalogTableStatisticsFuture ->
                                        catalogTableStatisticsFuture.cancel(true));
                        return Optional.empty();
                    }
                }
                List<CatalogTableStatistics> statsList = new ArrayList<>();
                for (Future<CatalogTableStatistics> future : statsFutureList) {
                    statsList.add(future.get());
                }
                HiveStatsUtil.updateStats(accumulate(statsList), statistic);
                return Optional.of(statistic);
            } else {
                // if the input format is neither OrcInputFormat nor MapredParquetInputFormat,
                // we can't gather full statistic in current implementation.
                // so return empty.
                return Optional.empty();
            }
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

        private boolean isStagingDir(Path path) {
            // in batch mode, the name for staging dir starts with '.staging_', see
            // HiveTableSink#toStagingDir
            // in stream mode, the stage dir is the partition/table location, but the staging files
            // start with '.'
            return path.getPath().startsWith(".");
        }

        private boolean isDataFile(FileStatus fileStatus) {
            String fileName = fileStatus.getPath().getName();
            return !fileName.startsWith(".")
                    && !fileName.startsWith("_")
                    && !fileName.equals(successFileName);
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

        private Future<CatalogTableStatistics> submitStatsGatherTask(
                Callable<CatalogTableStatistics> statsGatherTask) {
            if (executorService == null) {
                executorService =
                        gatherStatsThreadNum == 1
                                ? newDirectExecutorService()
                                : Executors.newFixedThreadPool(gatherStatsThreadNum);
            }
            return executorService.submit(statsGatherTask);
        }
    }

    /** Used to gather statistic for a single file. */
    static final class FileStatisticGather implements Callable<CatalogTableStatistics> {
        private final StatsProvidingRecordReader statsRR;
        private final FileStatus fileStatus;

        public FileStatisticGather(
                FileStatus fileStatus, StatsProvidingRecordReader statsProvidingRecordReader) {
            this.fileStatus = fileStatus;
            this.statsRR = statsProvidingRecordReader;
        }

        @Override
        public CatalogTableStatistics call() throws Exception {
            SerDeStats serDeStats = statsRR.getStats();
            return new CatalogTableStatistics(
                    serDeStats.getRowCount(),
                    1,
                    fileStatus.getLen(),
                    statsRR.getStats().getRawDataSize());
        }
    }

    private static CatalogTableStatistics accumulate(
            List<CatalogTableStatistics> catalogTableStatistics) {
        long rowCount = 0;
        int fileCount = 0;
        long totalSize = 0;
        long rawDataSize = 0;
        for (CatalogTableStatistics statistics : catalogTableStatistics) {
            rowCount += statistics.getRowCount();
            fileCount += statistics.getFileCount();
            totalSize += statistics.getTotalSize();
            rawDataSize += statistics.getRawDataSize();
        }
        return new CatalogTableStatistics(rowCount, fileCount, totalSize, rawDataSize);
    }
}
