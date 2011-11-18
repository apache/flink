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

package eu.stratosphere.nephele.plugins;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The plugin manager is responsible for loading and managing the individual plugins.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class PluginManager {

	/**
	 * The log object used to report errors and information in general.
	 */
	private static final Log LOG = LogFactory.getLog(PluginManager.class);

	/**
	 * The name of the file containing the plugin configuration.
	 */
	private static final String PLUGIN_CONFIG_FILE = "nephele-plugins.xml";

	/**
	 * The singleton instance of this class.
	 */
	private static PluginManager INSTANCE = null;

	private final Map<String, AbstractPluginLoader> plugins;

	private PluginManager(final String configDir) {

		// Check if the configuration file exists
		final File configFile = new File(configDir + File.separator + PLUGIN_CONFIG_FILE);
		if (configFile.exists()) {
			this.plugins = loadPlugins(configFile);
		} else {
			this.plugins = Collections.emptyMap();
			LOG.warn("Unable to load plugins: configuration file " + configFile.getAbsolutePath() + " not found");
		}
	}

	private String getTextChild(final Node node) {

		final NodeList nodeList = node.getChildNodes();
		if (nodeList.getLength() != 1) {
			return null;
		}

		final Node child = nodeList.item(0);
		if (!(child instanceof Text)) {
			return null;
		}
		final Text text = (Text) child;

		return text.getNodeValue();
	}

	@SuppressWarnings("unchecked")
	private Map<String, AbstractPluginLoader> loadPlugins(final File configFile) {

		final Map<String, AbstractPluginLoader> tmpPluginList = new LinkedHashMap<String, AbstractPluginLoader>();

		final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// Ignore comments in the XML file
		docBuilderFactory.setIgnoringComments(true);
		docBuilderFactory.setNamespaceAware(true);

		try {

			final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			final Document doc = builder.parse(configFile);

			if (doc == null) {
				LOG.error("Unable to load plugins: doc is null");
				return Collections.emptyMap();
			}

			final Element root = doc.getDocumentElement();
			if (root == null) {
				LOG.error("Unable to load plugins: root is null");
				return Collections.emptyMap();
			}

			if (!"plugins".equals(root.getNodeName())) {
				LOG.error("Unable to load plugins: unknown element " + root.getNodeName());
				return Collections.emptyMap();
			}

			final NodeList pluginNodes = root.getChildNodes();

			int pluginCounter = 0;
			for (int i = 0; i < pluginNodes.getLength(); ++i) {

				final Node pluginNode = pluginNodes.item(i);

				// Ignore text at this point
				if (pluginNode instanceof Text) {
					continue;
				}

				if (!"plugin".equals(pluginNode.getNodeName())) {
					LOG.error("Unable to load plugins: unknown element " + pluginNode.getNodeName());
					continue;
				}

				// Increase plugin counter
				++pluginCounter;

				final NodeList pluginChildren = pluginNode.getChildNodes();
				String pluginName = null;
				String pluginClass = null;
				Configuration pluginConfiguration = null;

				for (int j = 0; j < pluginChildren.getLength(); ++j) {

					final Node pluginChild = pluginChildren.item(j);

					// Ignore text at this point
					if (pluginChild instanceof Text) {
						continue;
					}

					if ("name".equals(pluginChild.getNodeName())) {
						pluginName = getTextChild(pluginChild);
						if (pluginName == null) {
							LOG.error("Skipping plugin " + pluginCounter
								+ " from configuration because it does not provide a proper name");
							continue;
						}
					}

					if ("class".equals(pluginChild.getNodeName())) {
						pluginClass = getTextChild(pluginChild);
						if (pluginClass == null) {
							LOG.error("Skipping plugin " + pluginCounter
								+ " from configuration because it does not provide a loader class");
							continue;
						}
					}

					if ("configuration".equals(pluginChild.getNodeName())) {

						pluginConfiguration = new Configuration();

						final NodeList configurationNodes = pluginChild.getChildNodes();
						for (int k = 0; k < configurationNodes.getLength(); ++k) {

							final Node configurationNode = configurationNodes.item(k);

							// Ignore text at this point
							if (configurationNode instanceof Text) {
								continue;
							}

							if (!"property".equals(configurationNode.getNodeName())) {
								LOG.error("Unexpected node " + configurationNode.getNodeName() + ", skipping...");
								continue;
							}

							String key = null;
							String value = null;

							final NodeList properties = configurationNode.getChildNodes();
							for (int l = 0; l < properties.getLength(); ++l) {

								final Node property = properties.item(l);

								// Ignore text at this point
								if (configurationNode instanceof Text) {
									continue;
								}

								if ("key".equals(property.getNodeName())) {
									key = getTextChild(property);
									if (key == null) {
										LOG.warn("Skipping configuration entry for plugin " + pluginName
											+ " because of invalid key");
										continue;
									}
								}

								if ("value".equals(property.getNodeName())) {
									value = getTextChild(property);
									if (value == null) {
										LOG.warn("Skipping configuration entry for plugin " + pluginName
											+ " because of invalid value");
										continue;
									}
								}
							}

							if (key != null && value != null) {
								pluginConfiguration.setString(key, value);
							}
						}

					}
				}

				if (pluginName == null) {
					LOG.error("Plugin " + pluginCounter + " does not provide a name, skipping...");
					continue;
				}

				if (pluginClass == null) {
					LOG.error("Plugin " + pluginCounter + " does not provide a loader class, skipping...");
					continue;
				}

				if (pluginConfiguration == null) {
					LOG.warn("Plugin " + pluginCounter
						+ " does not provide a configuration, using default configuration");
					pluginConfiguration = new Configuration();
				}

				Class<? extends AbstractPluginLoader> loaderClass;

				try {
					loaderClass = (Class<? extends AbstractPluginLoader>) Class.forName(pluginClass);
				} catch (ClassNotFoundException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				}

				if (loaderClass == null) {
					LOG.error("Unable to load plugin " + pluginName + ": loaderClass is null");
					continue;
				}

				Constructor<? extends AbstractPluginLoader> constructor;
				try {
					constructor = (Constructor<? extends AbstractPluginLoader>) loaderClass
						.getConstructor(Configuration.class);
				} catch (SecurityException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				} catch (NoSuchMethodException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				}

				if (constructor == null) {
					LOG.error("Unable to load plugin " + pluginName + ": constructor is null");
					continue;
				}

				AbstractPluginLoader pluginLoader = null;

				try {
					pluginLoader = constructor.newInstance(pluginConfiguration);
				} catch (IllegalArgumentException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				} catch (InstantiationException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				} catch (IllegalAccessException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				} catch (InvocationTargetException e) {
					LOG.error("Unable to load plugin " + pluginName + ": " + StringUtils.stringifyException(e));
					continue;
				}

				if (pluginLoader == null) {
					LOG.error("Unable to load plugin " + pluginName + ": pluginLoader is null");
					continue;
				}

				LOG.info("Successfully loaded plugin " + pluginName);
				tmpPluginList.put(pluginName, pluginLoader);
			}

		} catch (IOException e) {
			LOG.error("Error while loading plugins: " + StringUtils.stringifyException(e));
		} catch (SAXException e) {
			LOG.error("Error while loading plugins: " + StringUtils.stringifyException(e));
		} catch (ParserConfigurationException e) {
			LOG.error("Error while loading plugins: " + StringUtils.stringifyException(e));
		}

		return Collections.unmodifiableMap(tmpPluginList);
	}

	private static synchronized PluginManager getInstance(final String configDir) {

		if (INSTANCE == null) {
			INSTANCE = new PluginManager(configDir);
		}

		return INSTANCE;
	}

	private Map<PluginID, JobManagerPlugin> getJobManagerPluginsInternal() {

		final Map<PluginID, JobManagerPlugin> jobManagerPluginMap = new HashMap<PluginID, JobManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values().iterator();
		while (it.hasNext()) {

			final AbstractPluginLoader apl = it.next();
			final PluginID pluginID = apl.getPluginID();
			final JobManagerPlugin jmp = apl.getJobManagerPlugin();
			if (jmp != null) {
				if (!jobManagerPluginMap.containsKey(pluginID)) {
					jobManagerPluginMap.put(pluginID, jmp);
				} else {
					LOG.error("Detected ID collision for plugin " + apl.getPluginName() + ", skipping it...");
				}
			}
		}

		return Collections.unmodifiableMap(jobManagerPluginMap);
	}

	private Map<PluginID, TaskManagerPlugin> getTaskManagerPluginsInternal() {

		final Map<PluginID, TaskManagerPlugin> taskManagerPluginMap = new HashMap<PluginID, TaskManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values().iterator();
		while (it.hasNext()) {

			final AbstractPluginLoader apl = it.next();
			final PluginID pluginID = apl.getPluginID();
			final TaskManagerPlugin tmp = apl.getTaskManagerPlugin();
			if (tmp != null) {
				if (!taskManagerPluginMap.containsKey(pluginID)) {
					taskManagerPluginMap.put(pluginID, tmp);
				} else {
					LOG.error("Detected ID collision for plugin " + apl.getPluginName() + ", skipping it...");
				}
			}
		}

		return Collections.unmodifiableMap(taskManagerPluginMap);
	}

	public static Map<PluginID, JobManagerPlugin> getJobManagerPlugins(final String configDir) {

		return getInstance(configDir).getJobManagerPluginsInternal();
	}

	public static Map<PluginID, TaskManagerPlugin> getTaskManagerPlugins(final String configDir) {

		return getInstance(configDir).getTaskManagerPluginsInternal();
	}
}
