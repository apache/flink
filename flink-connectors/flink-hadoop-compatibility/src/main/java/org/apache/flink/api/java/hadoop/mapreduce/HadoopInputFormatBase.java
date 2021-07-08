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

package org.apache.flink.api.java.hadoop.mapreduce;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/** Base class shared between the Java and Scala API of Flink. */
@Internal
public abstract class HadoopInputFormatBase<K, V, T>
        extends HadoopInputFormatCommonBase<T, HadoopInputSplit> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatBase.class);

    // Mutexes to avoid concurrent operations on Hadoop InputFormats.
    // Hadoop parallelizes tasks across JVMs which is why they might rely on this JVM isolation.
    // In contrast, Flink parallelizes using Threads, so multiple Hadoop InputFormat instances
    // might be used in the same JVM.
    private static final Object OPEN_MUTEX = new Object();
    private static final Object CONFIGURE_MUTEX = new Object();
    private static final Object CLOSE_MUTEX = new Object();

    // NOTE: this class is using a custom serialization logic, without a defaultWriteObject()
    // method.
    // Hence, all fields here are "transient".
    private org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat;
    protected Class<K> keyClass;
    protected Class<V> valueClass;
    private org.apache.hadoop.conf.Configuration configuration;

    protected transient RecordReader<K, V> recordReader;
    protected boolean fetched = false;
    protected boolean hasNext;

    public HadoopInputFormatBase(
            org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
            Class<K> key,
            Class<V> value,
            Job job) {
        super(Preconditions.checkNotNull(job, "Job can not be null").getCredentials());
        this.mapreduceInputFormat = Preconditions.checkNotNull(mapreduceInputFormat);
        this.keyClass = Preconditions.checkNotNull(key);
        this.valueClass = Preconditions.checkNotNull(value);
        this.configuration = job.getConfiguration();
        HadoopUtils.mergeHadoopConf(configuration);
    }

    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return this.configuration;
    }

    // --------------------------------------------------------------------------------------------
    //  InputFormat
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {

        // enforce sequential configuration() calls
        synchronized (CONFIGURE_MUTEX) {
            if (mapreduceInputFormat instanceof Configurable) {
                ((Configurable) mapreduceInputFormat).setConf(configuration);
            }
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
        // only gather base statistics for FileInputFormats
        if (!(mapreduceInputFormat instanceof FileInputFormat)) {
            return null;
        }

        JobContext jobContext = new JobContextImpl(configuration, null);

        final FileBaseStatistics cachedFileStats =
                (cachedStats instanceof FileBaseStatistics)
                        ? (FileBaseStatistics) cachedStats
                        : null;

        try {
            final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(jobContext);
            return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
        } catch (IOException ioex) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Could not determine statistics due to an io error: " + ioex.getMessage());
            }
        } catch (Throwable t) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Unexpected problem while getting the file statistics: " + t.getMessage(),
                        t);
            }
        }

        // no statistics available
        return null;
    }

    @Override
    public HadoopInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        configuration.setInt("mapreduce.input.fileinputformat.split.minsize", minNumSplits);

        JobContext jobContext = new JobContextImpl(configuration, new JobID());

        jobContext.getCredentials().addAll(this.credentials);
        Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
        if (currentUserCreds != null) {
            jobContext.getCredentials().addAll(currentUserCreds);
        }

        List<org.apache.hadoop.mapreduce.InputSplit> splits;
        try {
            splits = this.mapreduceInputFormat.getSplits(jobContext);
        } catch (InterruptedException e) {
            throw new IOException("Could not get Splits.", e);
        }
        HadoopInputSplit[] hadoopInputSplits = new HadoopInputSplit[splits.size()];

        for (int i = 0; i < hadoopInputSplits.length; i++) {
            hadoopInputSplits[i] = new HadoopInputSplit(i, splits.get(i), jobContext);
        }
        return hadoopInputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(HadoopInputSplit split) throws IOException {

        // enforce sequential open() calls
        synchronized (OPEN_MUTEX) {
            TaskAttemptContext context =
                    new TaskAttemptContextImpl(configuration, new TaskAttemptID());

            try {
                this.recordReader =
                        this.mapreduceInputFormat.createRecordReader(
                                split.getHadoopInputSplit(), context);
                this.recordReader.initialize(split.getHadoopInputSplit(), context);
            } catch (InterruptedException e) {
                throw new IOException("Could not create RecordReader.", e);
            } finally {
                this.fetched = false;
            }
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (!this.fetched) {
            fetchNext();
        }
        return !this.hasNext;
    }

    protected void fetchNext() throws IOException {
        try {
            this.hasNext = this.recordReader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException("Could not fetch next KeyValue pair.", e);
        } finally {
            this.fetched = true;
        }
    }

    @Override
    public void close() throws IOException {
        if (this.recordReader != null) {

            // enforce sequential close() calls
            synchronized (CLOSE_MUTEX) {
                this.recordReader.close();
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Helper methods
    // --------------------------------------------------------------------------------------------

    private FileBaseStatistics getFileStats(
            FileBaseStatistics cachedStats,
            org.apache.hadoop.fs.Path[] hadoopFilePaths,
            ArrayList<FileStatus> files)
            throws IOException {

        long latestModTime = 0L;

        // get the file info and check whether the cached statistics are still valid.
        for (org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {

            final Path filePath = new Path(hadoopPath.toUri());
            final FileSystem fs = FileSystem.get(filePath.toUri());

            final FileStatus file = fs.getFileStatus(filePath);
            latestModTime = Math.max(latestModTime, file.getModificationTime());

            // enumerate all files and check their modification time stamp.
            if (file.isDir()) {
                FileStatus[] fss = fs.listStatus(filePath);
                files.ensureCapacity(files.size() + fss.length);

                for (FileStatus s : fss) {
                    if (!s.isDir()) {
                        files.add(s);
                        latestModTime = Math.max(s.getModificationTime(), latestModTime);
                    }
                }
            } else {
                files.add(file);
            }
        }

        // check whether the cached statistics are still valid, if we have any
        if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
            return cachedStats;
        }

        // calculate the whole length
        long len = 0;
        for (FileStatus s : files) {
            len += s.getLen();
        }

        // sanity check
        if (len <= 0) {
            len = BaseStatistics.SIZE_UNKNOWN;
        }

        return new FileBaseStatistics(latestModTime, len, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
    }

    // --------------------------------------------------------------------------------------------
    //  Custom serialization methods
    // --------------------------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        super.write(out);
        out.writeUTF(this.mapreduceInputFormat.getClass().getName());
        out.writeUTF(this.keyClass.getName());
        out.writeUTF(this.valueClass.getName());
        this.configuration.write(out);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.read(in);
        String hadoopInputFormatClassName = in.readUTF();
        String keyClassName = in.readUTF();
        String valueClassName = in.readUTF();

        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.readFields(in);

        if (this.configuration == null) {
            this.configuration = configuration;
        }

        try {
            this.mapreduceInputFormat =
                    (org.apache.hadoop.mapreduce.InputFormat<K, V>)
                            Class.forName(
                                            hadoopInputFormatClassName,
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate the hadoop input format", e);
        }
        try {
            this.keyClass =
                    (Class<K>)
                            Class.forName(
                                    keyClassName,
                                    true,
                                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new RuntimeException("Unable to find key class.", e);
        }
        try {
            this.valueClass =
                    (Class<V>)
                            Class.forName(
                                    valueClassName,
                                    true,
                                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new RuntimeException("Unable to find value class.", e);
        }
    }
}
