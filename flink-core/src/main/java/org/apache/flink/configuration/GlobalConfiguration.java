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

package org.apache.flink.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * Global configuration object for Flink. Similar to Java properties configuration
 * objects it includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

	/** The global configuration object accessible through a singleton pattern. */
	private static GlobalConfiguration SINGLETON = null;

	/** The internal map holding the key-value pairs the configuration consists of. */
	private final Configuration config = new Configuration();

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Retrieves the singleton object of the global configuration.
	 * 
	 * @return the global configuration object
	 */
	private static GlobalConfiguration get() {
		// lazy initialization currently only for testibility
		synchronized (GlobalConfiguration.class) {
			if (SINGLETON == null) {
				SINGLETON = new GlobalConfiguration();
			}
			return SINGLETON;
		}
	}

	/**
	 * The constructor used to construct the singleton instance of the global configuration.
	 */
	private GlobalConfiguration() {}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static String getString(String key, String defaultValue) {
		return get().config.getString(key, defaultValue);
	}

	/**
	 * Returns the value associated with the given key as a long integer.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static long getLong(String key, long defaultValue) {
		return get().config.getLong(key, defaultValue);
	}

	/**
	 * Returns the value associated with the given key as an integer.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static int getInteger(String key, int defaultValue) {
		return get().config.getInteger(key, defaultValue);
	}
	
	/**
	 * Returns the value associated with the given key as a float.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static float getFloat(String key, float defaultValue) {
		return get().config.getFloat(key, defaultValue);
	}

	/**
	 * Returns the value associated with the given key as a boolean.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static boolean getBoolean(String key, boolean defaultValue) {
		return get().config.getBoolean(key, defaultValue);
	}

	/**
	 * Loads the configuration files from the specified directory.
	 * <p>
	 * XML and YAML are supported as configuration files. If both XML and YAML files exist in the configuration
	 * directory, keys from YAML will overwrite keys from XML.
	 * 
	 * @param configDir
	 *        the directory which contains the configuration files
	 */
	public static void loadConfiguration(final String configDir) {

		if (configDir == null) {
			LOG.warn("Given configuration directory is null, cannot load configuration");
			return;
		}

		final File confDirFile = new File(configDir);
		if (!(confDirFile.exists())) {
			LOG.warn("The given configuration directory name '" + configDir + "' (" + confDirFile.getAbsolutePath()
				+ ") does not describe an existing directory.");
			return;
		}
		
		if (confDirFile.isFile()) {
			final File file = new File(configDir);
			if(configDir.endsWith(".xml")) {
				get().loadXMLResource( file );
			} else if(configDir.endsWith(".yaml")) {
				get().loadYAMLResource(file);
			} else {
				LOG.warn("The given configuration has an unknown extension.");
				return;
			}
			return;
		}

		// get all XML and YAML files in the directory
		final File[] xmlFiles = filterFilesBySuffix(confDirFile, ".xml");
		final File[] yamlFiles = filterFilesBySuffix(confDirFile, new String[] { ".yaml", ".yml" });

		if ((xmlFiles == null || xmlFiles.length == 0) && (yamlFiles == null || yamlFiles.length == 0)) {
			LOG.warn("Unable to get the contents of the config directory '" + configDir + "' ("
				+ confDirFile.getAbsolutePath() + ").");
			return;
		}

		// load config files and write into config map
		for (File f : xmlFiles) {
			get().loadXMLResource(f);
		}

		// => if both XML and YAML files exist, the YAML config keys overwrite XML settings
		for (File f : yamlFiles) {
			get().loadYAMLResource(f);
		}
	}

	/**
	 * Loads a YAML-file of key-value pairs.
	 * <p>
	 * Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a single-line comment.
	 * <p>
	 * Example:
	 * 
	 * <pre>
	 * jobmanager.rpc.address: localhost # network address for communication with the job manager
	 * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
	 * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
	 * </pre>
	 * <p>
	 * This does not span the whole YAML specification, but only the *syntax* of simple YAML key-value pairs (see issue
	 * #113 on GitHub). If at any point in time, there is a need to go beyond simple key-value pairs syntax
	 * compatibility will allow to introduce a YAML parser library.
	 * 
	 * @param file the YAML file to read from
	 * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
	 */
	private void loadYAMLResource(File file) {

		synchronized (getClass()) {

			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	
				String line = null;
				while ((line = reader.readLine()) != null) {
	
					// 1. check for comments
					String[] comments = line.split("#", 2);
					String conf = comments[0];
	
					// 2. get key and value
					if (conf.length() > 0) {
						String[] kv = conf.split(": ", 2);
	
						// skip line with no valid key-value pair
						if (kv.length == 1) {
							LOG.warn("Error while trying to split key and value in configuration file " + file + ": " + line);
							continue;
						}
	
						String key = kv[0].trim();
						String value = kv[1].trim();
						
						// sanity check
						if (key.length() == 0 || value.length() == 0) {
							LOG.warn("Error after splitting key and value in configuration file " + file + ": " + line);
							continue;
						}
	
						LOG.debug("Loading configuration property: {}, {}", key, value);
	
						this.config.setString(key, value);
					}
				}
			}
			catch (IOException e) {
				LOG.error("Error parsing YAML configuration.", e);
			}
			finally {
				try {
					if(reader != null) {
						reader.close();
					}
				} catch (IOException e) {
					LOG.warn("Cannot to close reader with IOException.", e);
				}
			}
		}
	}

	/**
	 * Loads an XML document of key-values pairs.
	 * 
	 * @param file
	 *        the XML document file
	 */
	private void loadXMLResource(File file) {

		final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// Ignore comments in the XML file
		docBuilderFactory.setIgnoringComments(true);
		docBuilderFactory.setNamespaceAware(true);

		try {

			final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			Document doc;
			Element root;

			doc = builder.parse(file);

			if (doc == null) {
				LOG.warn("Cannot load configuration: doc is null");
				return;
			}

			root = doc.getDocumentElement();
			if (root == null) {
				LOG.warn("Cannot load configuration: root is null");
				return;
			}

			if (!"configuration".equals(root.getNodeName())) {
				return;
			}

			final NodeList props = root.getChildNodes();
			int propNumber = -1;

			synchronized (getClass()) {

				for (int i = 0; i < props.getLength(); i++) {

					final Node propNode = props.item(i);
					String key = null;
					String value = null;

					// Ignore text at this point
					if (propNode instanceof Text) {
						continue;
					}

					if (!(propNode instanceof Element)) {
						continue;
					}

					Element property = (Element) propNode;
					if (!"property".equals(property.getNodeName())) {
						continue;
					}

					propNumber++;
					final NodeList propChildren = property.getChildNodes();
					if (propChildren == null) {
						LOG.warn("Error while reading configuration: property has no children, skipping...");
						continue;
					}

					for (int j = 0; j < propChildren.getLength(); j++) {

						final Node propChild = propChildren.item(j);
						if (propChild instanceof Element) {
							if ("key".equals(propChild.getNodeName())) {
								if (propChild.getChildNodes() != null) {
									if (propChild.getChildNodes().getLength() == 1) {
										if (propChild.getChildNodes().item(0) instanceof Text) {
											final Text t = (Text) propChild.getChildNodes().item(0);
											key = t.getTextContent();
										}
									}
								}
							}

							if ("value".equals(propChild.getNodeName())) {
								if (propChild.getChildNodes() != null) {
									if (propChild.getChildNodes().getLength() == 1) {
										if (propChild.getChildNodes().item(0) instanceof Text) {
											final Text t = (Text) propChild.getChildNodes().item(0);
											value = t.getTextContent();
										}
									}
								}
							}
						}
					}

					if (key != null && value != null) {
						// Put key, value pair into the map
						LOG.debug("Loading configuration property: {}, {}", key, value);
						this.config.setString(key, value);
					} else {
						LOG.warn("Error while reading configuration: Cannot read property " + propNumber);
					}
				}
			}

		}
		catch (Exception e) {
			LOG.error("Cannot load configuration.", e);
		}
	}

	/**
	 * Gets a {@link Configuration} object with the values of this GlobalConfiguration
	 * 
	 * @return the {@link Configuration} object including the key/value pairs
	 */
	public static Configuration getConfiguration() {
		Configuration copy = new Configuration();
		copy.addAll(get().config);
		return copy;
	}

	/**
	 * Merges the given {@link Configuration} object into the global
	 * configuration. If a key/value pair with an identical already
	 * exists in the global configuration, it is overwritten by the
	 * pair of the {@link Configuration} object.
	 * 
	 * @param conf
	 *        the {@link Configuration} object to merge into the global configuration
	 */
	public static void includeConfiguration(Configuration conf) {
		get().includeConfigurationInternal(conf);
	}

	/**
	 * Internal non-static method to include configuration.
	 * 
	 * @param conf
	 *        the {@link Configuration} object to merge into the global configuration
	 */
	private void includeConfigurationInternal(Configuration conf) {
		// static synchronized
		synchronized (getClass()) {
			this.config.addAll(conf);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Filters files in directory which have the specified suffix (e.g. ".xml").
	 * 
	 * @param dirToFilter
	 *        directory to filter
	 * @param suffix
	 *        suffix to filter files by (e.g. ".xml")
	 * @return files with given ending in directory
	 */
	private static File[] filterFilesBySuffix(final File dirToFilter, final String suffix) {
		return filterFilesBySuffix(dirToFilter, new String[] { suffix });
	}

	private static File[] filterFilesBySuffix(final File dirToFilter, final String[] suffixes) {
		return dirToFilter.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				for (String suffix : suffixes) {
					if (dir.equals(dirToFilter) && name != null && name.endsWith(suffix)) {
						return true;
					}
				}

				return false;
			}
		});
	}
}
