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

package org.apache.flink.tachyon;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.AvroOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

/**
 * This test should logically be located in the 'flink-runtime' tests. However, this project
 * has already all dependencies required (flink-java-examples). Also, the ParallelismOneExecEnv is here.
 */
public class HDFSTest {

	private String hdfsURI;
	private MiniDFSCluster hdfsCluster;
	private org.apache.hadoop.fs.Path hdPath;
	private org.apache.hadoop.fs.FileSystem hdfs;

	@Before
	public void createHDFS() {
		try {
			Configuration hdConf = new Configuration();

			File baseDir = new File("./target/hdfs/hdfsTest").getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() +"/";

			hdPath = new org.apache.hadoop.fs.Path("/test");
			hdfs = hdPath.getFileSystem(hdConf);
			FSDataOutputStream stream = hdfs.create(hdPath);
			for(int i = 0; i < 10; i++) {
				stream.write("Hello HDFS\n".getBytes());
			}
			stream.close();

		} catch(Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@After
	public void destroyHDFS() {
		try {
			hdfs.delete(hdPath, false);
			hdfsCluster.shutdown();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Test
	public void testHDFS() {

		Path file = new Path(hdfsURI + hdPath);
		org.apache.hadoop.fs.Path result = new org.apache.hadoop.fs.Path(hdfsURI + "/result");
		try {
			FileSystem fs = file.getFileSystem();
			Assert.assertTrue("Must be HadoopFileSystem", fs instanceof HadoopFileSystem);
			new TachyonFileSystemWrapperTest.DopOneTestEnvironment();
			try {
				WordCount.main(new String[]{file.toString(), result.toString()});
			} catch(Throwable t) {
				t.printStackTrace();
				Assert.fail("Test failed with " + t.getMessage());
			}
			Assert.assertTrue("No result file present", hdfs.exists(result));
			// validate output:
			org.apache.hadoop.fs.FSDataInputStream inStream = hdfs.open(result);
			StringWriter writer = new StringWriter();
			IOUtils.copy(inStream, writer);
			String resultString = writer.toString();

			Assert.assertEquals("(hdfs,10)\n" +
					"(hello,10)\n", resultString);
			inStream.close();

		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Error in test: " + e.getMessage() );
		}
	}

	@Test
	public void testAvroOut() {
		String type = "one";
		AvroOutputFormat<String> avroOut =
				new AvroOutputFormat<String>( String.class );

		org.apache.hadoop.fs.Path result = new org.apache.hadoop.fs.Path(hdfsURI + "/avroTest");

		avroOut.setOutputFilePath(new Path(result.toString()));
		avroOut.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE);
		avroOut.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);

		try {
			avroOut.open(0, 2);
			avroOut.writeRecord(type);
			avroOut.close();

			avroOut.open(1, 2);
			avroOut.writeRecord(type);
			avroOut.close();


			Assert.assertTrue("No result file present", hdfs.exists(result));
			FileStatus[] files = hdfs.listStatus(result);
			Assert.assertEquals(2, files.length);
			for(FileStatus file : files) {
				Assert.assertTrue("1.avro".equals(file.getPath().getName()) || "2.avro".equals(file.getPath().getName()));
			}

		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
