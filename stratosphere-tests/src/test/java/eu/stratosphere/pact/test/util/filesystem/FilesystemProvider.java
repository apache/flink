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

package eu.stratosphere.pact.test.util.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FilesystemProvider {

	public void start() throws Exception;
	
	public void stop();
	
	public boolean createFile(String fileName, String fileContent) throws IOException;

	public boolean copyFile(String localFile, String hdfsFile) throws IOException;
	
	public boolean createDir(String dirName) throws IOException;

	public boolean delete(String path, boolean recursive) throws IOException;
	
	public OutputStream getOutputStream(String file) throws IOException;

	public InputStream getInputStream(String file) throws IOException;
	
	public String getTempDirPath();
	
	public String[] listFiles(String dir) throws IOException;
	
	public boolean isDir(String file) throws IOException;
	
	public String getURIPrefix();
}
