/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.util.tests;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.pact.test.util.filesystem.HDFSProvider;
import eu.stratosphere.pact.test.util.filesystem.MiniDFSProvider;
import eu.stratosphere.pact.test.util.minicluster.NepheleMiniCluster;

/**
 * @author Erik Nijkamp
 */
public class TestNepheleMiniCluster extends TestCase {
	private HDFSProvider hdfs;

	private NepheleMiniCluster nephele;

	@BeforeClass
	public void setUp() throws Exception {
		hdfs = new MiniDFSProvider();
		hdfs.start();

		String nepheleConfigDir = System.getProperty("user.dir") + "/tmp/nephele/config";
		String hdfsConfigDir = hdfs.getConfigDir();
		nephele = new NepheleMiniCluster(nepheleConfigDir, hdfsConfigDir, 1 /*
																			 * task-
																			 * tracker
																			 */, 128 /* memory */);
	}

	@Test
	public void test() throws Exception {
		preSubmit();
		nephele.submitJobAndWait(getJobGraph());
		postSubmit();
	}

	protected void preSubmit() throws Exception {
		OutputStream os = hdfs.getOutputStream("input.txt");
		Writer wr = new OutputStreamWriter(os);
		wr.write("hello\n");
		wr.write("foo\n");
		wr.write("bar\n");
		wr.close();
	}

	protected JobGraph getJobGraph() throws Exception {
		JobGraph jobGraph = new JobGraph("Grep Example Job");

		JobFileInputVertex input = new JobFileInputVertex("Output 1", jobGraph);
		input.setFileInputClass(FileLineReader.class);
		input.setFilePath(new Path(hdfs.getTempDirPath() + "/input.txt"));

		JobTaskVertex task = new JobTaskVertex("Task 1", jobGraph);
		task.setTaskClass(GrepTask.class);

		JobFileOutputVertex output = new JobFileOutputVertex("Output 1", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path(hdfs.getTempDirPath() + "/output.txt"));

		input.connectTo(task, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		task.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		return jobGraph;
	}

	protected void postSubmit() throws Exception {
		InputStream is = hdfs.getInputStream(hdfs.getTempDirPath() + "/output.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("no output", line);
		while (line != null) {
			Assert.assertTrue(line.contains("hello") || line.contains("foo") || line.contains("bar"));
			System.out.println("### >>> out = " + line);
			line = reader.readLine();
		}
		reader.close();
	}

	@Ignore
	public static class FileLineWriter extends AbstractFileOutputTask {
		private RecordReader<StringRecord> input;

		@Override
		public void registerInputOutput() {
			input = new RecordReader<StringRecord>(this, StringRecord.class, null);
		}

		@Override
		public void invoke() throws Exception {
			Path path = new Path("hdfs://localhost:9000/user/" + System.getProperty("user.name").toLowerCase()
				+ "/output.txt");
			eu.stratosphere.nephele.fs.FileSystem fs = path.getFileSystem();

			FSDataOutputStream outputStream = fs.create(path, true);

			while (input.hasNext()) {
				StringRecord record = input.next();
				byte[] recordByte = (record.toString() + "\n").getBytes();
				outputStream.write(recordByte, 0, recordByte.length);
			}

			outputStream.close();
		}
	}

	@Ignore
	public static class GrepTask extends AbstractTask {
		private RecordReader<StringRecord> input;

		private RecordWriter<StringRecord> output;

		@Override
		public void registerInputOutput() {
			this.input = new RecordReader<StringRecord>(this, StringRecord.class, null);
			this.output = new RecordWriter<StringRecord>(this, StringRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			System.out.println("## GrepTask.invoke()");
			while (this.input.hasNext()) {
				StringRecord string = this.input.next();
				System.out.println("## GrepTask.invoke() -> line -> " + string.toString());
				this.output.emit(string);
			}
		}
	}
}
