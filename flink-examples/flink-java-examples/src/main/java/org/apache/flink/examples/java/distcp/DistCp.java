package org.apache.flink.examples.java.distcp;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.com.google.common.base.Stopwatch;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zholudev on 27/08/15.
 * Class that uses dynamic input format to copy files over
 */
public class DistCp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistCp.class);

    public static void main(String[] args) throws Exception {
        final Path sourcePath = new Path(args[0]);
        final Path targetPath = new Path(args[1]);
        int parallelism = Integer.valueOf(args[2], 10);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        Stopwatch stopwatch = new Stopwatch().start();
        LOGGER.info("Initializing copy tasks");
        List<FileCopyTask> tasks = getCopyTasks(sourcePath);
        LOGGER.info("Copy task initialization took " + stopwatch.elapsedTime(TimeUnit.MILLISECONDS) + "ms");

        DataSet<FileCopyTask> inputTasks = new DataSource<>(env,
                new FileCopyTaskInputFormat(tasks),
                new GenericTypeInfo<>(FileCopyTask.class), "fileCopyTasks");


        FlatMapOperator<FileCopyTask, Object> res = inputTasks.flatMap(new FlatMapFunction<FileCopyTask, Object>() {
            @Override
            public void flatMap(FileCopyTask task, Collector<Object> out) throws Exception {
                LOGGER.info("Processing task: " + task);
                Path outPath = new Path(targetPath, task.getRelativePath());

                FileSystem targetFs = targetPath.getFileSystem();
                if (!targetFs.isDistributedFS()) {
                    File parentFile = new File(outPath.toUri()).getParentFile();
                    if (parentFile.mkdirs()) {
                        throw new RuntimeException("Cannot create local file system directories: " + parentFile);
                    }
                }

                FSDataOutputStream outputStream = null;
                FSDataInputStream inputStream = null;
                try {
                    outputStream = targetFs.create(outPath, true);
                    inputStream = task.getPath().getFileSystem().open(task.getPath());
                    IOUtils.copy(inputStream, outputStream);
                } finally {
                    IOUtils.closeQuietly(inputStream);
                    IOUtils.closeQuietly(outputStream);
                }
            }
        });

        // executing the program. Since it does not have an explicit output, nothing needs to be written to a sink
        res.print();
    }

    private static List<FileCopyTask> getCopyTasks(Path sourcePath) throws IOException {
        List<FileCopyTask> tasks = new ArrayList<>();
        getCopyTasks(sourcePath, "", tasks);
        return tasks;
    }

    private static void getCopyTasks(Path p, String rel, List<FileCopyTask> tasks) throws IOException {
        FileStatus[] res = p.getFileSystem().listStatus(p);
        if (res == null) {
            return;
        }
        for (FileStatus fs : res) {
            if (fs.isDir()) {
                getCopyTasks(fs.getPath(), rel + fs.getPath().getName() + "/", tasks);
            } else {
                Path cp = fs.getPath();
                tasks.add(new FileCopyTask(cp, rel + cp.getName()));
            }
        }
    }

}
