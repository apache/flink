/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.util.filesystem;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HDFSProvider implements FilesystemProvider {
	
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
	
	public OutputStream getOutputStream(String file) throws IOException {
		return hdfs.create(new org.apache.hadoop.fs.Path(file));
	}

	public InputStream getInputStream(String file) throws IOException {
		return hdfs.open(new org.apache.hadoop.fs.Path(file));
	}

	public String getTempDirPath() {
		return "/user/"+hdfs.getHomeDirectory().getName();
	}

	public boolean createDir(String dirName) throws IOException {
		if(!getFileSystem().mkdirs(new Path(dirName))) {
			return false;
		}
		
		while (!getFileSystem().exists(new Path(dirName))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public boolean delete(String path, boolean recursive) throws IOException {
		if(!getFileSystem().delete(new Path(path), recursive)) {
			return false;
		}
		
		while (getFileSystem().exists(new Path(path))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public boolean createFile(String fileName, String fileContent) throws IOException {
		if(getFileSystem().exists(new Path(fileName))) {
			return false;
		}
		
		OutputStream os = this.getOutputStream(fileName);
		Writer wr = new OutputStreamWriter(os);
		wr.write(fileContent);
		wr.close();
		while (!getFileSystem().exists(new Path(fileName))) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public boolean copyFile(String localFile, String hdfsFile) throws IOException {
		if(getFileSystem().exists(new Path(hdfsFile))) {
			return false;
		}
		
		FileReader fr = new FileReader(new File(localFile));
		OutputStream os = this.getOutputStream(hdfsFile);
		Writer wr = new OutputStreamWriter(os);

		while (fr.ready()) {
			wr.write(fr.read());
		}
		wr.close();
		fr.close();
		
		return true;
	}
	
	public String[] listFiles(String dir) throws IOException {
		FileStatus[] fss = hdfs.listStatus(new Path(dir));
		ArrayList<String> fileList = new ArrayList<String>(fss.length);
		for(FileStatus fs : fss) {
			fileList.add(fs.getPath().toString());
		}		
		return fileList.toArray(new String[1]);
	}
	
	public boolean isDir(String file) throws IOException {
		return hdfs.getFileStatus(new Path(file)).isDir();
	}
	
	public String getURIPrefix() {
		return hdfs.getUri().toString();
	}
	
}
