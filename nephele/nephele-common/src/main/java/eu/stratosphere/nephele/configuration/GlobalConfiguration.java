/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.StringUtils;

/**
 * Global configuration object in Nephele. Similar to a Java properties configuration
 * objects it includes key-value pairs which represent the framework's settings/configuration.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class GlobalConfiguration {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(GlobalConfiguration.class);

	/**
	 * The global configuration object accessible through a singleton pattern.
	 */
	private static final GlobalConfiguration CONFIGURATION = new GlobalConfiguration();

	/**
	 * The key to the directory this configuration was read from.
	 */
	private static final String CONFIG_DIR_KEY = "config.dir";

	/**
	 * The internal map holding the key-value pairs the configuration consists of.
	 */
	private final Map<String, String> confData = new ConcurrentHashMap<String, String>();

	/**
	 * The constructor used to construct the singleton instance of the global configuration.
	 */
	private GlobalConfiguration() {
	}

	/**
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        the key pointing to the associated value
	 * @param defaultValue
	 *        the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	public static String getString(final String key, final String defaultValue) {

		return CONFIGURATION.getStringInternal(key, defaultValue);
	}

	/**
	 * Returns the value associated with the given key as a string.
	 * 
	 * @param key
	 *        key the key pointing to the associated value
	 * @param defaultValue
	 *        defaultValue the default value which is returned in case there is no value associated with the given key
	 * @return the (default) value associated with the given key
	 */
	private String getStringInternal(final String key, final String defaultValue) {

		final String retVal = this.confData.get(key);
		if (retVal == null) {
			return defaultValue;
		}

		return retVal;
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
	public static long getLong(final String key, final long defaultValue) {

		return CONFIGURATION.getLongInternal(key, defaultValue);
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
	private long getLongInternal(final String key, final long defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		try {
			return Long.parseLong(str);
		} catch (NumberFormatException nfe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(nfe));
			}
		}

		return defaultValue;
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
	public static int getInteger(final String key, final int defaultValue) {

		return CONFIGURATION.getIntegerInternal(key, defaultValue);
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
	private int getIntegerInternal(final String key, final int defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		try {
			return Integer.parseInt(str);
		} catch (NumberFormatException nfe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(StringUtils.stringifyException(nfe));
			}
		}

		return defaultValue;
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
	public static boolean getBoolean(final String key, final boolean defaultValue) {

		return CONFIGURATION.getBooleanInternal(key, defaultValue);
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
	private boolean getBooleanInternal(final String key, final boolean defaultValue) {

		final String str = this.confData.get(key);
		if (str == null) {
			return defaultValue;
		}

		return Boolean.parseBoolean(str);
	}

	/**
	 * Loads the configuration files from the specified directory.
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
		if (!(confDirFile.exists() && confDirFile.isDirectory())) {
			LOG.warn("The given configuration directory name '" + configDir + "'(" + confDirFile.getAbsolutePath()
				+ ") does not describe an existing directory.");
			return;
		}

		// get all XML files in the directory
		final File[] files = confDirFile.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return dir == confDirFile && name != null && name.endsWith(".xml");
			}

		});

		if (files == null || files.length == 0) {
			LOG.warn("Unable to get the contents of the config directory '" + configDir + "' ("
				+ confDirFile.getAbsolutePath() + ").");
			return;
		}

		// load each xml file
		for (File f : files) {
			CONFIGURATION.loadResource(f);
		}

		// Store the path to the configuration directory itself
		CONFIGURATION.confData.put(CONFIG_DIR_KEY, configDir);
	}

	/**
	 * Loads an XML document of key-values pairs.
	 * 
	 * @param file
	 *        the XML document file
	 */
	private void loadResource(final File file) {

		final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// Ignore comments in the XML file
		docBuilderFactory.setIgnoringComments(true);
		docBuilderFactory.setNamespaceAware(true);

		// TODO: Trying to set this option causes an exception. What do we need it for? (DW)
		/*
		 * try {
		 * docBuilderFactory.setXIncludeAware(true);
		 * } catch (UnsupportedOperationException e) {
		 * LOG.error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory + ":" + e, e);
		 * }
		 */

		try {

			final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			Document doc = null;
			Element root = null;

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
				LOG.warn("Cannot load configuration: unknown element " + root.getNodeName());
				return;
			}

			final NodeList props = root.getChildNodes();
			int propNumber = -1;

			for (int i = 0; i < props.getLength(); i++) {

				final Node propNode = props.item(i);
				String key = null;
				String value = null;

				// Ignore text at this point
				if (propNode instanceof Text) {
					continue;
				}

				if (!(propNode instanceof Element)) {
					LOG.warn("Error while reading configuration: " + propNode.getNodeName()
						+ " is not of type element");
					continue;
				}

				Element property = (Element) propNode;
				if (!"property".equals(property.getNodeName())) {
					LOG.warn("Error while reading configuration: unknown element " + property.getNodeName());
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
						if ("key".equals(propChild.getNodeName()) && propChild.getChildNodes() != null
							&& propChild.getChildNodes().getLength() == 1
							&& propChild.getChildNodes().item(0) instanceof Text) {

							final Text t = (Text) propChild.getChildNodes().item(0);
							key = t.getTextContent();
						}

						if ("value".equals(propChild.getNodeName()) && propChild.getChildNodes() != null
							&& propChild.getChildNodes().getLength() == 1
							&& propChild.getChildNodes().item(0) instanceof Text) {

							final Text t = (Text) propChild.getChildNodes().item(0);
							value = t.getTextContent();
						}
					}
				}

				if (key != null && value != null) {
					// Put key, value pair into the map
					LOG.debug("Loading configuration property: " + key + ", " + value);
					this.confData.put(key, value);
				} else {
					LOG.warn("Error while reading configuration: Cannot read property " + propNumber);
					continue;
				}
			}

		} catch (ParserConfigurationException e) {
			LOG.warn("Cannot load configuration: " + StringUtils.stringifyException(e));
		} catch (IOException e) {
			LOG.warn("Cannot load configuration: " + StringUtils.stringifyException(e));
		} catch (SAXException e) {
			LOG.warn("Cannot load configuration: " + StringUtils.stringifyException(e));
		}
	}

	/**
	 * Copies the key/value pairs stored in the global configuration to
	 * a {@link Configuration} object and returns it.
	 * 
	 * @return the {@link Configuration} object including the key/value pairs
	 */
	public static Configuration getConfiguration() {

		return CONFIGURATION.getConfigurationInternal(null);
	}

	/**
	 * Copies a subset of the key/value pairs stored in the global configuration to
	 * a {@link Configuration} object and returns it. The subset is defined by the
	 * given array of keys. If <code>keys</code> is <code>null</code>, the entire
	 * global configuration is copied.
	 * 
	 * @param keys
	 *        array of keys specifying the subset of pairs to copy.
	 * @return the {@link Configuration} object including the key/value pairs
	 */
	public static Configuration getConfiguration(final String[] keys) {

		return CONFIGURATION.getConfigurationInternal(keys);
	}

	/**
	 * Internal non-static method to return configuration.
	 * 
	 * @param keys
	 *        array of keys specifying the subset of pairs to copy.
	 * @return the {@link Configuration} object including the key/value pairs
	 */
	private Configuration getConfigurationInternal(final String[] keys) {

		if (keys == null) {
			return new Configuration(this.confData);
		}

		final Configuration configuration = new Configuration();
		for (int i = 0; i < keys.length; ++i) {
			final String key = keys[i];
			final String value = this.confData.get(key);
			if (value != null) {
				configuration.setString(key, value);
			}
		}

		return configuration;
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
	public static void includeConfiguration(final Configuration conf) {

		CONFIGURATION.includeConfigurationInternal(conf);
	}

	/**
	 * Internal non-static method to include configuration.
	 * 
	 * @param conf
	 *        the {@link Configuration} object to merge into the global configuration
	 */
	private void includeConfigurationInternal(final Configuration conf) {

		if (conf == null) {
			throw new NullPointerException("Given configuration object is null, ignoring it...");
		}

		final Iterator<String> it = conf.keySet().iterator();

		while (it.hasNext()) {
			final String key = it.next();
			this.confData.put(key, conf.getString(key, ""));
		}
	}
}
