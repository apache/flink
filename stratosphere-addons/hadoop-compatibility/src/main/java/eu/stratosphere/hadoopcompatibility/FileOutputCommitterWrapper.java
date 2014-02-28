package eu.stratosphere.hadoopcompatibility;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

/**
 * Hadoop 1.2.1 {@link org.apache.hadoop.mapred.FileOutputCommitter} takes {@link org.apache.hadoop.mapred.JobContext}
 * as input parameter. However JobContext class is package private, and in Hadoop 2.2.0 it's public.
 * This class takes {@link org.apache.hadoop.mapred.JobConf} as input instead of JobContext in order to setup and commit tasks.
 */
public class FileOutputCommitterWrapper extends FileOutputCommitter implements Serializable {

	static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
		"mapreduce.fileoutputcommitter.marksuccessfuljobs";

	public void setupJob(JobConf conf) throws IOException {
		Path outputPath = FileOutputFormat.getOutputPath(conf);
		if (outputPath != null) {
			Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
			FileSystem fileSys = tmpDir.getFileSystem(conf);
			if (!fileSys.mkdirs(tmpDir)) {
				LOG.error("Mkdirs failed to create " + tmpDir.toString());
			}
		}
	}

	private static boolean getOutputDirMarking(JobConf conf) {
		return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
			true);
	}

	private void markSuccessfulOutputDir(JobConf conf)
		throws IOException {
		Path outputPath = FileOutputFormat.getOutputPath(conf);
		if (outputPath != null) {
			FileSystem fileSys = outputPath.getFileSystem(conf);
			// create a file in the folder to mark it
			if (fileSys.exists(outputPath)) {
				Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
				fileSys.create(filePath).close();
			}
		}
	}

	private Path getFinalPath(Path jobOutputDir, Path taskOutput,
							  Path taskOutputPath) throws IOException {
		URI taskOutputUri = taskOutput.toUri();
		URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
		if (taskOutputUri == relativePath) {//taskOutputPath is not a parent of taskOutput
			throw new IOException("Can not get the relative path: base = " +
				taskOutputPath + " child = " + taskOutput);
		}
		if (relativePath.getPath().length() > 0) {
			return new Path(jobOutputDir, relativePath.getPath());
		} else {
			return jobOutputDir;
		}
	}
	private void moveTaskOutputs(JobConf conf, TaskAttemptID taskAttemptID,
								 FileSystem fs,
								 Path jobOutputDir,
								 Path taskOutput)
		throws IOException {
		if (fs.isFile(taskOutput)) {
			Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput,
				getTempTaskOutputPath(conf, taskAttemptID));
			if (!fs.rename(taskOutput, finalOutputPath)) {
				if (!fs.delete(finalOutputPath, true)) {
					throw new IOException("Failed to delete earlier output of task: " +
						taskAttemptID);
				}
				if (!fs.rename(taskOutput, finalOutputPath)) {
					throw new IOException("Failed to save output of task: " +
						taskAttemptID);
				}
			}
			LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
		} else if(fs.getFileStatus(taskOutput).isDir()) {
			FileStatus[] paths = fs.listStatus(taskOutput);
			Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput,
				getTempTaskOutputPath(conf, taskAttemptID));
			fs.mkdirs(finalOutputPath);
			if (paths != null) {
				for (FileStatus path : paths) {
					moveTaskOutputs(conf,taskAttemptID, fs, jobOutputDir, path.getPath());
				}
			}
		}
	}

	public void commitTask(JobConf conf, TaskAttemptID taskAttemptID)
		throws IOException {
		Path taskOutputPath = getTempTaskOutputPath(conf, taskAttemptID);
		if (taskOutputPath != null) {
			FileSystem fs = taskOutputPath.getFileSystem(conf);
			if (fs.exists(taskOutputPath)) {
				Path jobOutputPath = taskOutputPath.getParent().getParent();
				// Move the task outputs to their final place
				moveTaskOutputs(conf,taskAttemptID, fs, jobOutputPath, taskOutputPath);
				// Delete the temporary task-specific output directory
				if (!fs.delete(taskOutputPath, true)) {
					LOG.info("Failed to delete the temporary output" +
						" directory of task: " + taskAttemptID + " - " + taskOutputPath);
				}
				LOG.info("Saved output of task '" + taskAttemptID + "' to " +
					jobOutputPath);
			}
		}
	}
	public boolean needsTaskCommit(JobConf conf, TaskAttemptID taskAttemptID)
		throws IOException {
		try {
			Path taskOutputPath = getTempTaskOutputPath(conf, taskAttemptID);
			if (taskOutputPath != null) {
				// Get the file-system for the task output directory
				FileSystem fs = taskOutputPath.getFileSystem(conf);
				// since task output path is created on demand,
				// if it exists, task needs a commit
				if (fs.exists(taskOutputPath)) {
					return true;
				}
			}
		} catch (IOException  ioe) {
			throw ioe;
		}
		return false;
	}

	public Path getTempTaskOutputPath(JobConf conf, TaskAttemptID taskAttemptID) {
		Path outputPath = FileOutputFormat.getOutputPath(conf);
		if (outputPath != null) {
			Path p = new Path(outputPath,
				(FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
					"_" + taskAttemptID.toString()));
			try {
				FileSystem fs = p.getFileSystem(conf);
				return p.makeQualified(fs);
			} catch (IOException ie) {
				LOG.warn(StringUtils.stringifyException(ie));
				return p;
			}
		}
		return null;
	}
	public void cleanupJob(JobConf conf) throws IOException {
		// do the clean up of temporary directory
		Path outputPath = FileOutputFormat.getOutputPath(conf);
		if (outputPath != null) {
			Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
			FileSystem fileSys = tmpDir.getFileSystem(conf);
			if (fileSys.exists(tmpDir)) {
				fileSys.delete(tmpDir, true);
			}
		} else {
			LOG.warn("Output path is null in cleanup");
		}
	}

	public void commitJob(JobConf conf) throws IOException {
		cleanupJob(conf);
		if (getOutputDirMarking(conf)) {
			markSuccessfulOutputDir(conf);
		}
	}

}
