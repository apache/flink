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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import eu.stratosphere.pact.test.util.FileWriter;

/**
 * @author Erik Nijkamp
 */
public class MiniDFSProvider extends HDFSProvider {
	private MiniDFSCluster cluster;

	private final static int NAME_NODE_PORT = 9000;

	public MiniDFSProvider() {
		super(getTempDir() + "/config");
	}

	public void start() throws Exception {
		deleteDir(getTempDir());

		// create dirs
		new FileWriter().dir(getTempDir());
		new FileWriter().dir(getLogDir());
		new FileWriter().dir(getDfsDir());
		new FileWriter().dir(getConfigDir());

		// set system properies needed by the MiniDFSCluster
		System.setProperty("hadoop.log.dir", getLogDir());
		System.setProperty("test.build.data", getDfsDir());

		// init hdfs cluster
		cluster = new MiniDFSCluster(NAME_NODE_PORT, new Configuration(), 1, true, true, null, null);
		hdfs = cluster.getFileSystem();

		// write hdfs config files needed by nephele in temp folder
		new FileWriter().dir(getConfigDir()).file("hadoop-site.xml").write(
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
			"<?xml-stylesheet href=\"configuration.xsl\" type=\"text/xsl\"?>", "<configuration>", "   <property>",
			"       <name>fs.default.name</name>",
			"       <value>" + "hdfs://localhost:" + cluster.getNameNodePort() + "</value>", "   </property>",
			"   <property>", "       <name>dfs.name.dir</name>", "       <value>" + getDfsDir() + "/name</value>",
			"   </property>", "   <property>", "       <name>dfs.data.dir</name>",
			"       <value>" + getDfsDir() + "/data</value>", "   </property>", "</configuration>").close().file(
			"hadoop-default.xml").write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
			"<?xml-stylesheet href=\"configuration.xsl\" type=\"text/xsl\"?>", "<configuration>", "</configuration>")
			.close();
	}

	public void stop() {

		try {
			hdfs.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		cluster.shutdown();

		deleteDir(getTempDir());

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {

		}
	}

	private static String getTempDir() {
		return System.getProperty("user.dir") + "/tmp";
	}

	private static String getDfsDir() {
		return getTempDir() + "/dfs";
	}

	private static String getLogDir() {
		return getTempDir() + "/logs";
	}

	private boolean deleteDir(String dir) {
		return deleteDir(new File(dir));
	}

	private boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] childrens = dir.list();
			for (String child : childrens) {
				boolean success = deleteDir(new File(dir, child));
				if (!success) {
					return false;
				}
			}
		}

		return dir.delete();
	}
}
