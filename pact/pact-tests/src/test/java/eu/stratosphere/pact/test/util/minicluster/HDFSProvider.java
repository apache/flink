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

package eu.stratosphere.pact.test.util.minicluster;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HDFSProvider {
	protected FileSystem hdfs;

	protected final String configDir;

	abstract public void start() throws Exception;

	abstract public void stop();

	protected HDFSProvider(String config) {
		this.configDir = config;
	}

	public FileSystem getFileSystem() {
		return hdfs;
	}

	public String getConfigDir() {
		return configDir;
	}

	public OutputStream getHdfsOutputStream(String file) throws IOException {
		return hdfs.create(new org.apache.hadoop.fs.Path(file));
	}

	public InputStream getHdfsInputStream(String file) throws IOException {
		return hdfs.open(new org.apache.hadoop.fs.Path(file));
	}

	public String getHdfsHome() {
		return hdfs.getHomeDirectory().toString();
	}

	public String getHdfsRoot() {
		return hdfs.getUri().toString();
	}

	public String getHdfsDir(String dir) {
		return hdfs.getUri().toString() + dir;
	}

	public void createDir(String dirName) throws IOException {
		getFileSystem().mkdirs(new Path(dirName));
		while (!getFileSystem().exists(new Path(dirName))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void delete(String path, boolean recursive) throws IOException {
		getFileSystem().delete(new Path(path), recursive);
		while (getFileSystem().exists(new Path(path))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void writeFileToHDFS(String fileName, String fileContent) throws IOException {
		OutputStream os = this.getHdfsOutputStream(fileName);
		Writer wr = new OutputStreamWriter(os);
		wr.write(fileContent);
		wr.close();
		while (!getFileSystem().exists(new Path(fileName))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void copyLocalFileToHDFS(String localFile, String hdfsFile) throws IOException {
		FileReader fr = new FileReader(new File(localFile));
		OutputStream os = this.getHdfsOutputStream(hdfsFile);
		Writer wr = new OutputStreamWriter(os);

		while (fr.ready()) {
			wr.write(fr.read());
		}
		wr.close();
		fr.close();
	}
}
