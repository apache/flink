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

package eu.stratosphere.yarn;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;

public class Utils {
	
	private static final Log LOG = LogFactory.getLog(Utils.class);
	

	public static void copyJarContents(String prefix, String pathToJar) throws IOException {
		LOG.info("Copying jar (location: "+pathToJar+") to prefix "+prefix);
		
		JarFile jar = null;
		jar = new JarFile(pathToJar);
		Enumeration<JarEntry> enumr = jar.entries();
		byte[] bytes = new byte[1024];
		while(enumr.hasMoreElements()) {
			JarEntry entry = enumr.nextElement();
			if(entry.getName().startsWith(prefix)) {
				if(entry.isDirectory()) {
					File cr = new File(entry.getName());
					cr.mkdirs();
					continue;
				}
				InputStream inStream = jar.getInputStream(entry);
				File outFile = new File(entry.getName());
				if(outFile.exists()) {
					throw new RuntimeException("File unexpectedly exists");
				}
				FileOutputStream outputStream = new FileOutputStream(outFile);
				int read = 0;
				while ((read = inStream.read(bytes)) != -1) {
					outputStream.write(bytes, 0, read);
				}
				inStream.close(); outputStream.close(); 
			}
		}
		jar.close();
	}
	
	public static void getStratosphereConfiguration(String confDir) {
		GlobalConfiguration.loadConfiguration(confDir);
	}
	
	private static void addPathToConfig(Configuration conf, File path) {
		// chain-in a new classloader
		URL fileUrl = null;
		try {
			fileUrl = path.toURL();
		} catch (MalformedURLException e) {
			throw new RuntimeException("Erroneous config file path", e);
		}
		URL[] urls = {fileUrl};
		ClassLoader cl = new URLClassLoader(urls, conf.getClassLoader());
		conf.setClassLoader(cl);
	}
	private static void setDefaultConfValues(Configuration conf) {
		if(conf.get("fs.hdfs.impl",null) == null) {
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		}
		if(conf.get("fs.file.impl",null) == null) {
			conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		}
	}
	public static Configuration initializeYarnConfiguration() {
		Configuration conf = new YarnConfiguration();
		String configuredHadoopConfig = GlobalConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
		if(configuredHadoopConfig != null) {
			LOG.info("Using hadoop configuration path from " + ConfigConstants.PATH_HADOOP_CONFIG + " setting.");
			addPathToConfig(conf, new File(configuredHadoopConfig));
			setDefaultConfValues(conf);
			return conf;
		}
		String[] envs = { "YARN_CONF_DIR", "HADOOP_CONF_DIR", "HADOOP_CONF_PATH" };
		for(int i = 0; i < envs.length; ++i) {
			String confPath = System.getenv(envs[i]);
			if (confPath != null) {
				LOG.info("Found "+envs[i]+", adding it to configuration");
				addPathToConfig(conf, new File(confPath));
				setDefaultConfValues(conf);
				return conf;
			}
		}
		LOG.info("Could not find HADOOP_CONF_PATH, using HADOOP_HOME.");
		String hadoopHome = null;
		try {
			hadoopHome = Shell.getHadoopHome();
		} catch (IOException e) {
			LOG.fatal("Unable to get hadoop home. Please set HADOOP_HOME variable!", e);
			System.exit(1);
		}
		File tryConf = new File(hadoopHome+"/etc/hadoop");
		if(tryConf.exists()) {
			LOG.info("Found configuration using hadoop home.");
			addPathToConfig(conf, tryConf);
		} else {
			tryConf = new File(hadoopHome+"/conf");
			if(tryConf.exists()) {
				addPathToConfig(conf, tryConf);
			}
		}
		setDefaultConfValues(conf);
		return conf;
	}
	
	public static void setupEnv(Configuration conf, Map<String, String> appMasterEnv) {
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
	}
	
	
	/**
	 * 
	 * @return Path to remote file (usually hdfs)
	 * @throws IOException
	 */
	public static Path setupLocalResource(Configuration conf, FileSystem fs, String appId, Path localRsrcPath, LocalResource appMasterJar, Path homedir)
			throws IOException {
		// copy to HDFS
		String suffix = ".stratosphere/" + appId + "/" + localRsrcPath.getName();
		
		Path dst = new Path(homedir, suffix);
		
		LOG.info("Copying from "+localRsrcPath+" to "+dst );
		fs.copyFromLocalFile(localRsrcPath, dst);
		registerLocalResource(fs, dst, appMasterJar);
		return dst;
	}
	
	public static void registerLocalResource(FileSystem fs, Path remoteRsrcPath, LocalResource localResource) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(jarStat.getLen());
		localResource.setTimestamp(jarStat.getModificationTime());
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(LocalResourceVisibility.PUBLIC);
	}

	public static void setTokensFor(ContainerLaunchContext amContainer, Path[] paths, Configuration conf) throws IOException {
		Credentials credentials = new Credentials();
		// for HDFS
		TokenCache.obtainTokensForNamenodes(credentials, paths, conf);
		// for user
		UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();
		
		Collection<Token<? extends TokenIdentifier>> usrTok = currUsr.getTokens();
		for(Token<? extends TokenIdentifier> token : usrTok) {
			final Text id = new Text(token.getIdentifier());
			LOG.info("Adding user token "+id+" with "+token);
			credentials.addToken(id, token);
		}
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		LOG.debug("Wrote tokens. Credentials buffer length: "+dob.getLength());
		
		ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		amContainer.setTokens(securityTokens);
	}
	
	public static void logFilesInCurrentDirectory(final Log logger) {
		new File(".").list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				logger.info(dir.getAbsolutePath()+"/"+name);
				return true;
			}
		});
	}
}
