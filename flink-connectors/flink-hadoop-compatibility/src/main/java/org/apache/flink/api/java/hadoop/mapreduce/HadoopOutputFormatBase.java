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
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.java.hadoop.common.HadoopOutputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase.getCredentialsFromUGI;

/** Base class shared between the Java and Scala API of Flink. */
@Internal
public abstract class HadoopOutputFormatBase<K, V, T> extends HadoopOutputFormatCommonBase<T>
        implements FinalizeOnMaster {

    private static final long serialVersionUID = 1L;

    // Mutexes to avoid concurrent operations on Hadoop OutputFormats.
    // Hadoop parallelizes tasks across JVMs which is why they might rely on this JVM isolation.
    // In contrast, Flink parallelizes using Threads, so multiple Hadoop OutputFormat instances
    // might be used in the same JVM.
    protected static final Object OPEN_MUTEX = new Object();
    protected static final Object CONFIGURE_MUTEX = new Object();
    protected static final Object CLOSE_MUTEX = new Object();

    protected org.apache.hadoop.conf.Configuration configuration;
    protected org.apache.hadoop.mapreduce.OutputFormat<K, V> mapreduceOutputFormat;
    protected transient RecordWriter<K, V> recordWriter;
    protected transient OutputCommitter outputCommitter;
    protected transient TaskAttemptContext context;
    protected transient int taskNumber;

    public HadoopOutputFormatBase(
            org.apache.hadoop.mapreduce.OutputFormat<K, V> mapreduceOutputFormat, Job job) {
        super(job.getCredentials());
        this.mapreduceOutputFormat = mapreduceOutputFormat;
        this.configuration = job.getConfiguration();
        HadoopUtils.mergeHadoopConf(configuration);
    }

    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return this.configuration;
    }

    // --------------------------------------------------------------------------------------------
    //  OutputFormat
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {

        // enforce sequential configure() calls
        synchronized (CONFIGURE_MUTEX) {
            if (this.mapreduceOutputFormat instanceof Configurable) {
                ((Configurable) this.mapreduceOutputFormat).setConf(this.configuration);
            }
        }
    }

    /**
     * create the temporary output file for hadoop RecordWriter.
     *
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     * @throws java.io.IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        // enforce sequential open() calls
        synchronized (OPEN_MUTEX) {
            if (Integer.toString(taskNumber + 1).length() > 6) {
                throw new IOException("Task id too large.");
            }

            this.taskNumber = taskNumber + 1;

            // for hadoop 2.2
            this.configuration.set("mapreduce.output.basename", "tmp");

            TaskAttemptID taskAttemptID =
                    TaskAttemptID.forName(
                            "attempt__0000_r_"
                                    + String.format(
                                                    "%"
                                                            + (6
                                                                    - Integer.toString(
                                                                                    taskNumber + 1)
                                                                            .length())
                                                            + "s",
                                                    " ")
                                            .replace(" ", "0")
                                    + Integer.toString(taskNumber + 1)
                                    + "_0");

            this.configuration.set("mapred.task.id", taskAttemptID.toString());
            this.configuration.setInt("mapred.task.partition", taskNumber + 1);
            // for hadoop 2.2
            this.configuration.set("mapreduce.task.attempt.id", taskAttemptID.toString());
            this.configuration.setInt("mapreduce.task.partition", taskNumber + 1);

            try {
                this.context = new TaskAttemptContextImpl(this.configuration, taskAttemptID);
                this.outputCommitter = this.mapreduceOutputFormat.getOutputCommitter(this.context);
                this.outputCommitter.setupJob(new JobContextImpl(this.configuration, new JobID()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            this.context.getCredentials().addAll(this.credentials);
            Credentials currentUserCreds =
                    getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
            if (currentUserCreds != null) {
                this.context.getCredentials().addAll(currentUserCreds);
            }

            // compatible for hadoop 2.2.0, the temporary output directory is different from hadoop
            // 1.2.1
            if (outputCommitter instanceof FileOutputCommitter) {
                this.configuration.set(
                        "mapreduce.task.output.dir",
                        ((FileOutputCommitter) this.outputCommitter).getWorkPath().toString());
            }

            try {
                this.recordWriter = this.mapreduceOutputFormat.getRecordWriter(this.context);
            } catch (InterruptedException e) {
                throw new IOException("Could not create RecordWriter.", e);
            }
        }
    }

    /**
     * commit the task by moving the output file out from the temporary directory.
     *
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {

        // enforce sequential close() calls
        synchronized (CLOSE_MUTEX) {
            try {
                this.recordWriter.close(this.context);
            } catch (InterruptedException e) {
                throw new IOException("Could not close RecordReader.", e);
            }

            if (this.outputCommitter.needsTaskCommit(this.context)) {
                this.outputCommitter.commitTask(this.context);
            }

            Path outputPath = new Path(this.configuration.get("mapred.output.dir"));

            // rename tmp-file to final name
            FileSystem fs = FileSystem.get(outputPath.toUri(), this.configuration);

            String taskNumberStr = Integer.toString(this.taskNumber);
            String tmpFileTemplate = "tmp-r-00000";
            String tmpFile =
                    tmpFileTemplate.substring(0, 11 - taskNumberStr.length()) + taskNumberStr;

            if (fs.exists(new Path(outputPath.toString() + "/" + tmpFile))) {
                fs.rename(
                        new Path(outputPath.toString() + "/" + tmpFile),
                        new Path(outputPath.toString() + "/" + taskNumberStr));
            }
        }
    }

    @Override
    public void finalizeGlobal(int parallelism) throws IOException {

        JobContext jobContext;
        TaskAttemptContext taskContext;
        try {
            TaskAttemptID taskAttemptID =
                    TaskAttemptID.forName(
                            "attempt__0000_r_"
                                    + String.format(
                                                    "%" + (6 - Integer.toString(1).length()) + "s",
                                                    " ")
                                            .replace(" ", "0")
                                    + Integer.toString(1)
                                    + "_0");

            jobContext = new JobContextImpl(this.configuration, new JobID());
            taskContext = new TaskAttemptContextImpl(this.configuration, taskAttemptID);
            this.outputCommitter = this.mapreduceOutputFormat.getOutputCommitter(taskContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        jobContext.getCredentials().addAll(this.credentials);
        Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
        if (currentUserCreds != null) {
            jobContext.getCredentials().addAll(currentUserCreds);
        }

        // finalize HDFS output format
        if (this.outputCommitter != null) {
            this.outputCommitter.commitJob(jobContext);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Custom serialization methods
    // --------------------------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        super.write(out);
        out.writeUTF(this.mapreduceOutputFormat.getClass().getName());
        this.configuration.write(out);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.read(in);
        String hadoopOutputFormatClassName = in.readUTF();

        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.readFields(in);

        if (this.configuration == null) {
            this.configuration = configuration;
        }

        try {
            this.mapreduceOutputFormat =
                    (org.apache.hadoop.mapreduce.OutputFormat<K, V>)
                            Class.forName(
                                            hadoopOutputFormatClassName,
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate the hadoop output format", e);
        }
    }
}
