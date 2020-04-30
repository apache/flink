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

package org.apache.flink.api.java;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

/**
 * Provides utility services for jarring and unjarring files and directories. Note that a given instance of JarHelper is
 * not threadsafe with respect to multiple jar operations.
 *
 * <p>Copied from http://grepcode.com/file_/repo1.maven.org/maven2/org.apache.xmlbeans/xmlbeans/2.4.0/org/apache/xmlbeans/impl/common/JarHelper.java/?v=source
 *
 * <p>Author: Patrick Calahan <a href="mailto:pcal@bea.com">pcal@bea.com</a>
 */
public class JarHelper {
	// ========================================================================
	// Constants

	private static final int BUFFER_SIZE = 2156;

	// ========================================================================
	// Variables

	private byte[] mBuffer = new byte[BUFFER_SIZE];

	private int mByteCount = 0;

	private boolean mVerbose = false;

	private String mDestJarName = "";

	// ========================================================================
	// Constructor

	/**
	 * Instantiates a new JarHelper.
	 */
	public JarHelper() {
	}

	// ========================================================================
	// Public methods

	/**
	 * Jars a given directory or single file into a JarOutputStream.
	 */
	public void jarDir(File dirOrFile2Jar, File destJar)
		throws IOException {

		if (dirOrFile2Jar == null || destJar == null) {
			throw new IllegalArgumentException();
		}

		mDestJarName = destJar.getCanonicalPath();
		FileOutputStream fout = new FileOutputStream(destJar);
		JarOutputStream jout = new JarOutputStream(fout);
		//jout.setLevel(0);
		try {
			jarDir(dirOrFile2Jar, jout, null);
		} catch (IOException ioe) {
			throw ioe;
		} finally {
			jout.close();
			fout.close();
		}
	}

	/**
	 * Unjars a given jar file into a given directory.
	 */
	public void unjarDir(File jarFile, File destDir) throws IOException {
		BufferedOutputStream dest = null;
		FileInputStream fis = new FileInputStream(jarFile);
		unjar(fis, destDir);
	}

	/**
	 * Given an InputStream on a jar file, unjars the contents into the given
	 * directory.
	 */
	public void unjar(InputStream in, File destDir) throws IOException {
		BufferedOutputStream dest = null;
		JarInputStream jis = new JarInputStream(in);
		JarEntry entry;
		while ((entry = jis.getNextJarEntry()) != null) {
			if (entry.isDirectory()) {
				File dir = new File(destDir, entry.getName());
				dir.mkdir();
				if (entry.getTime() != -1) {
					dir.setLastModified(entry.getTime());
				}
				continue;
			}
			int count;
			byte[] data = new byte[BUFFER_SIZE];
			File destFile = new File(destDir, entry.getName());
			if (mVerbose) {
				System.out.println("unjarring " + destFile +
					" from " + entry.getName());
			}
			FileOutputStream fos = new FileOutputStream(destFile);
			dest = new BufferedOutputStream(fos, BUFFER_SIZE);
			try {
				while ((count = jis.read(data, 0, BUFFER_SIZE)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
			} finally {
				dest.close();
			}
			if (entry.getTime() != -1) {
				destFile.setLastModified(entry.getTime());
			}
		}
		jis.close();
	}

	public void setVerbose(boolean b) {
		mVerbose = b;
	}

	// ========================================================================
	// Private methods

	private static final char SEP = '/';

	/**
	 * Recursively jars up the given path under the given directory.
	 */
	private void jarDir(File dirOrFile2jar, JarOutputStream jos, String path)
		throws IOException {
		if (mVerbose) {
			System.out.println("checking " + dirOrFile2jar);
		}
		if (dirOrFile2jar.isDirectory()) {
			String[] dirList = dirOrFile2jar.list();
			String subPath = (path == null) ? "" : (path + dirOrFile2jar.getName() + SEP);
			if (path != null) {
				JarEntry je = new JarEntry(subPath);
				je.setTime(dirOrFile2jar.lastModified());
				jos.putNextEntry(je);
				jos.flush();
				jos.closeEntry();
			}
			for (int i = 0; i < dirList.length; i++) {
				File f = new File(dirOrFile2jar, dirList[i]);
				jarDir(f, jos, subPath);
			}
		} else if (dirOrFile2jar.exists()) {
			if (dirOrFile2jar.getCanonicalPath().equals(mDestJarName)) {
				if (mVerbose) {
					System.out.println("skipping " + dirOrFile2jar.getPath());
				}
				return;
			}

			if (mVerbose) {
				System.out.println("adding " + dirOrFile2jar.getPath());
			}
			FileInputStream fis = new FileInputStream(dirOrFile2jar);
			try {
				JarEntry entry = new JarEntry(path + dirOrFile2jar.getName());
				entry.setTime(dirOrFile2jar.lastModified());
				jos.putNextEntry(entry);
				while ((mByteCount = fis.read(mBuffer)) != -1) {
					jos.write(mBuffer, 0, mByteCount);
					if (mVerbose) {
						System.out.println("wrote " + mByteCount + " bytes");
					}
				}
				jos.flush();
				jos.closeEntry();
			} catch (IOException ioe) {
				throw ioe;
			} finally {
				fis.close();
			}
		}
	}

	// for debugging
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Usage: JarHelper jarname.jar directory");
			return;
		}

		JarHelper jarHelper = new JarHelper();
		jarHelper.mVerbose = true;

		File destJar = new File(args[0]);
		File dirOrFile2Jar = new File(args[1]);

		jarHelper.jarDir(dirOrFile2Jar, destJar);
	}
}
