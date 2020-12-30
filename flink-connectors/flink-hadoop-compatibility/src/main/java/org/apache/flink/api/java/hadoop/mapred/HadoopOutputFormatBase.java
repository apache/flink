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

package org.apache.flink.api.java.hadoop.mapred;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.java.hadoop.common.HadoopOutputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyProgressable;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase.getCredentialsFromUGI;

/**
 * Common base for the mapred HadoopOutputFormat wrappers. There are implementations for Java and
 * Scala.
 *
 * @param <K> Type of Key
 * @param <V> Type of Value
 * @param <T> Record type.
 */
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

    protected JobConf jobConf;
    protected org.apache.hadoop.mapred.OutputFormat<K, V> mapredOutputFormat;
    protected transient RecordWriter<K, V> recordWriter;
    protected transient OutputCommitter outputCommitter;
    protected transient TaskAttemptContext context;

    public HadoopOutputFormatBase(
            org.apache.hadoop.mapred.OutputFormat<K, V> mapredOutputFormat, JobConf job) {
        super(job.getCredentials());
        this.mapredOutputFormat = mapredOutputFormat;
        HadoopUtils.mergeHadoopConf(job);
        this.jobConf = job;
    }

    public JobConf getJobConf() {
        return jobConf;
    }

    // --------------------------------------------------------------------------------------------
    //  OutputFormat
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {

        // enforce sequential configure() calls
        synchronized (CONFIGURE_MUTEX) {
            // configure MR OutputFormat if necessary
            if (this.mapredOutputFormat instanceof Configurable) {
                ((Configurable) this.mapredOutputFormat).setConf(this.jobConf);
            } else if (this.mapredOutputFormat instanceof JobConfigurable) {
                ((JobConfigurable) this.mapredOutputFormat).configure(this.jobConf);
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

            this.jobConf.set("mapred.task.id", taskAttemptID.toString());
            this.jobConf.setInt("mapred.task.partition", taskNumber + 1);
            // for hadoop 2.2
            this.jobConf.set("mapreduce.task.attempt.id", taskAttemptID.toString());
            this.jobConf.setInt("mapreduce.task.partition", taskNumber + 1);

            this.context = new TaskAttemptContextImpl(this.jobConf, taskAttemptID);

            this.outputCommitter = this.jobConf.getOutputCommitter();

            JobContext jobContext = new JobContextImpl(this.jobConf, new JobID());

            this.outputCommitter.setupJob(jobContext);

            this.recordWriter =
                    this.mapredOutputFormat.getRecordWriter(
                            null,
                            this.jobConf,
                            Integer.toString(taskNumber + 1),
                            new HadoopDummyProgressable());
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
            this.recordWriter.close(new HadoopDummyReporter());

            if (this.outputCommitter.needsTaskCommit(this.context)) {
                this.outputCommitter.commitTask(this.context);
            }
        }
    }

    @Override
    public void finalizeGlobal(int parallelism) throws IOException {

        try {
            JobContext jobContext = new JobContextImpl(this.jobConf, new JobID());
            OutputCommitter outputCommitter = this.jobConf.getOutputCommitter();

            // finalize HDFS output format
            outputCommitter.commitJob(jobContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Custom serialization methods
    // --------------------------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        super.write(out);
        out.writeUTF(mapredOutputFormat.getClass().getName());
        jobConf.write(out);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        super.read(in);
        String hadoopOutputFormatName = in.readUTF();
        if (jobConf == null) {
            jobConf = new JobConf();
        }
        jobConf.readFields(in);
        try {
            this.mapredOutputFormat =
                    (org.apache.hadoop.mapred.OutputFormat<K, V>)
                            Class.forName(
                                            hadoopOutputFormatName,
                                            true,
                                            Thread.currentThread().getContextClassLoader())
                                    .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate the hadoop output format", e);
        }
        ReflectionUtils.setConf(mapredOutputFormat, jobConf);

        jobConf.getCredentials().addAll(this.credentials);
        Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
        if (currentUserCreds != null) {
            jobConf.getCredentials().addAll(currentUserCreds);
        }
    }
}
