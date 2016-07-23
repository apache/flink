/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.fs.util;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *  This is a utility class to impose recursive copy from local directory when using {@link FileSystem} implementations
 *  as we cannot assume that all FileSystem implementations will implement a recursive copy.
 *  Presently,  <a "href"="https://github.com/Aloisius/hadoop-s3a">S3a</a>does not.
 */
public class FileSystemCopyUtils {

	/**
	 * Recursive copy to work around FileSystem implementations that do not implement it.
	 * @param fs
	 * @param localPath
	 * @param remotePath
	 * @throws IOException
	 */
	public static void copyFromLocalFile(FileSystem fs, boolean overwrite, Path localPath, Path remotePath) throws IOException {
		localPath = checkFileScheme(localPath);
		File localFile = new File(localPath.toUri());
		if (localFile.isDirectory()) {
			for (File file : localFile.listFiles()) {
				copyFromLocalFile(fs, overwrite, new Path("file://" + file.getAbsolutePath()), new Path(remotePath,file.getName()));
			}
		} else {
			fs.copyFromLocalFile(false, overwrite, localPath,remotePath);
		}
	}

	/**
	 * All paths
	 * @param localRsrcPath
	 * @return
	 * @throws IOException
	 */
	private static Path checkFileScheme(Path localRsrcPath) throws IOException {
		if (localRsrcPath.isAbsoluteAndSchemeAuthorityNull()) {
			try {
				return new Path(new URI("file://" + localRsrcPath.toString()));
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		}
		return localRsrcPath;
	}

}
