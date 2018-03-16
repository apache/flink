/**
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

package org.apache.hadoop.conf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** 
 * Provides access to configuration parameters.
 *
 * <h4 id="Resources">Resources</h4>
 *
 * <p>Configurations are specified by resources. A resource contains a set of
 * name/value pairs as XML data. Each resource is named by either a 
 * <code>String</code> or by a {@link Path}. If named by a <code>String</code>, 
 * then the classpath is examined for a file with that name.  If named by a 
 * <code>Path</code>, then the local filesystem is examined directly, without 
 * referring to the classpath.
 *
 * <p>Unless explicitly turned off, Hadoop by default specifies two 
 * resources, loaded in-order from the classpath: <ol>
 * <li><tt>
 * <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default-shaded.xml">
 * core-default-shaded.xml</a></tt>: Read-only defaults for hadoop.</li>
 * <li><tt>core-site.xml</tt>: Site-specific configuration for a given hadoop
 * installation.</li>
 * </ol>
 * Applications may add additional resources, which are loaded
 * subsequent to these resources in the order they are added.
 * 
 * <h4 id="FinalParams">Final Parameters</h4>
 *
 * <p>Configuration parameters may be declared <i>final</i>. 
 * Once a resource declares a value final, no subsequently-loaded 
 * resource can alter that value.  
 * For example, one might define a final parameter with:
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;dfs.hosts.include&lt;/name&gt;
 *    &lt;value&gt;/etc/hadoop/conf/hosts.include&lt;/value&gt;
 *    <b>&lt;final&gt;true&lt;/final&gt;</b>
 *  &lt;/property&gt;</pre></tt>
 *
 * Administrators typically define parameters as final in 
 * <tt>core-site.xml</tt> for values that user applications may not alter.
 *
 * <h4 id="VariableExpansion">Variable Expansion</h4>
 *
 * <p>Value strings are first processed for <i>variable expansion</i>. The
 * available properties are:<ol>
 * <li>Other properties defined in this Configuration; and, if a name is
 * undefined here,</li>
 * <li>Properties in {@link System#getProperties()}.</li>
 * </ol>
 *
 * <p>For example, if a configuration resource contains the following property
 * definitions: 
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *  
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;</pre></tt>
 *
 * When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt>
 * will be resolved to another property in this Configuration, while
 * <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value
 * of the System property with that name.
 * <p>When <tt>conf.get("otherdir")</tt> is called, then <tt>${<i>env.BASE_DIR</i>}</tt>
 * will be resolved to the value of the <tt>${<i>BASE_DIR</i>}</tt> environment variable.
 * It supports <tt>${<i>env.NAME:-default</i>}</tt> and <tt>${<i>env.NAME-default</i>}</tt> notations.
 * The former is resolved to "default" if <tt>${<i>NAME</i>}</tt> environment variable is undefined
 * or its value is empty.
 * The latter behaves the same way only if <tt>${<i>NAME</i>}</tt> is undefined.
 * <p>By default, warnings will be given to any deprecated configuration 
 * parameters and these are suppressible by configuring
 * <tt>log4j.logger.org.apache.hadoop.conf.Configuration.deprecation</tt> in
 * log4j.properties file.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Configuration implements Iterable<Map.Entry<String,String>>,
                                      Writable {
  private static final Log LOG =
    LogFactory.getLog(Configuration.class);

  private static final Log LOG_DEPRECATION =
    LogFactory.getLog("org.apache.hadoop.conf.Configuration.deprecation");

  private boolean quietmode = true;

  private static final String DEFAULT_STRING_CHECK =
    "testingforemptydefaultvalue";

  private boolean allowNullValueProperties = false;
  
  private static class Resource {
    private final Object resource;
    private final String name;
    
    public Resource(Object resource) {
      this(resource, resource.toString());
    }
    
    public Resource(Object resource, String name) {
      this.resource = resource;
      this.name = name;
    }
    
    public String getName(){
      return name;
    }
    
    public Object getResource() {
      return resource;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  /**
   * List of configuration resources.
   */
  private ArrayList<Resource> resources = new ArrayList<Resource>();
  
  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource by dumpConfiguration.
   */
  static final String UNKNOWN_RESOURCE = "Unknown";


  /**
   * List of configuration parameters marked <b>final</b>. 
   */
  private Set<String> finalParameters = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());
  
  private boolean loadDefaults = true;
  
  /**
   * Configuration objects
   */
  private static final WeakHashMap<Configuration,Object> REGISTRY = 
    new WeakHashMap<Configuration,Object>();
  
  /**
   * List of default Resources. Resources are loaded in the order of the list 
   * entries
   */
  private static final CopyOnWriteArrayList<String> defaultResources =
    new CopyOnWriteArrayList<String>();

  private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>>
    CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, WeakReference<Class<?>>>>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final Class<?> NEGATIVE_CACHE_SENTINEL =
    NegativeCacheSentinel.class;

  /**
   * Stores the mapping of key to the resource which modifies or loads 
   * the key most recently
   */
  private Map<String, String[]> updatingResource;
 
  /**
   * Class to keep the information about the keys which replace the deprecated
   * ones.
   * 
   * This class stores the new keys which replace the deprecated keys and also
   * gives a provision to have a custom message for each of the deprecated key
   * that is being replaced. It also provides method to get the appropriate
   * warning message which can be logged whenever the deprecated key is used.
   */
  private static class DeprecatedKeyInfo {
    private final String[] newKeys;
    private final String customMessage;
    private final AtomicBoolean accessed = new AtomicBoolean(false);

    DeprecatedKeyInfo(String[] newKeys, String customMessage) {
      this.newKeys = newKeys;
      this.customMessage = customMessage;
    }

    /**
     * Method to provide the warning message. It gives the custom message if
     * non-null, and default message otherwise.
     * @param key the associated deprecated key.
     * @return message that is to be logged when a deprecated key is used.
     */
    private final String getWarningMessage(String key) {
      String warningMessage;
      if(customMessage == null) {
        StringBuilder message = new StringBuilder(key);
        String deprecatedKeySuffix = " is deprecated. Instead, use ";
        message.append(deprecatedKeySuffix);
        for (int i = 0; i < newKeys.length; i++) {
          message.append(newKeys[i]);
          if(i != newKeys.length-1) {
            message.append(", ");
          }
        }
        warningMessage = message.toString();
      }
      else {
        warningMessage = customMessage;
      }
      return warningMessage;
    }

    boolean getAndSetAccessed() {
      return accessed.getAndSet(true);
    }

    public void clearAccessed() {
      accessed.set(false);
    }
  }
  
  /**
   * A pending addition to the global set of deprecated keys.
   */
  public static class DeprecationDelta {
    private final String key;
    private final String[] newKeys;
    private final String customMessage;

    DeprecationDelta(String key, String[] newKeys, String customMessage) {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(newKeys);
      Preconditions.checkArgument(newKeys.length > 0);
      this.key = key;
      this.newKeys = newKeys;
      this.customMessage = customMessage;
    }

    public DeprecationDelta(String key, String newKey, String customMessage) {
      this(key, new String[] { newKey }, customMessage);
    }

    public DeprecationDelta(String key, String newKey) {
      this(key, new String[] { newKey }, null);
    }

    public String getKey() {
      return key;
    }

    public String[] getNewKeys() {
      return newKeys;
    }

    public String getCustomMessage() {
      return customMessage;
    }
  }

  /**
   * The set of all keys which are deprecated.
   *
   * DeprecationContext objects are immutable.
   */
  private static class DeprecationContext {
    /**
     * Stores the deprecated keys, the new keys which replace the deprecated keys
     * and custom message(if any provided).
     */
    private final Map<String, DeprecatedKeyInfo> deprecatedKeyMap;

    /**
     * Stores a mapping from superseding keys to the keys which they deprecate.
     */
    private final Map<String, String> reverseDeprecatedKeyMap;

    /**
     * Create a new DeprecationContext by copying a previous DeprecationContext
     * and adding some deltas.
     *
     * @param other   The previous deprecation context to copy, or null to start
     *                from nothing.
     * @param deltas  The deltas to apply.
     */
    @SuppressWarnings("unchecked")
    DeprecationContext(DeprecationContext other, DeprecationDelta[] deltas) {
      HashMap<String, DeprecatedKeyInfo> newDeprecatedKeyMap = 
        new HashMap<String, DeprecatedKeyInfo>();
      HashMap<String, String> newReverseDeprecatedKeyMap =
        new HashMap<String, String>();
      if (other != null) {
        for (Entry<String, DeprecatedKeyInfo> entry :
            other.deprecatedKeyMap.entrySet()) {
          newDeprecatedKeyMap.put(entry.getKey(), entry.getValue());
        }
        for (Entry<String, String> entry :
            other.reverseDeprecatedKeyMap.entrySet()) {
          newReverseDeprecatedKeyMap.put(entry.getKey(), entry.getValue());
        }
      }
      for (DeprecationDelta delta : deltas) {
        if (!newDeprecatedKeyMap.containsKey(delta.getKey())) {
          DeprecatedKeyInfo newKeyInfo =
            new DeprecatedKeyInfo(delta.getNewKeys(), delta.getCustomMessage());
          newDeprecatedKeyMap.put(delta.key, newKeyInfo);
          for (String newKey : delta.getNewKeys()) {
            newReverseDeprecatedKeyMap.put(newKey, delta.key);
          }
        }
      }
      this.deprecatedKeyMap =
        UnmodifiableMap.decorate(newDeprecatedKeyMap);
      this.reverseDeprecatedKeyMap =
        UnmodifiableMap.decorate(newReverseDeprecatedKeyMap);
    }

    Map<String, DeprecatedKeyInfo> getDeprecatedKeyMap() {
      return deprecatedKeyMap;
    }

    Map<String, String> getReverseDeprecatedKeyMap() {
      return reverseDeprecatedKeyMap;
    }
  }
  
  private static DeprecationDelta[] defaultDeprecations = 
    new DeprecationDelta[] {
      new DeprecationDelta("topology.script.file.name", 
        CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY),
      new DeprecationDelta("topology.script.number.args", 
        CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY),
      new DeprecationDelta("hadoop.configured.node.mapping", 
        CommonConfigurationKeys.NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY),
      new DeprecationDelta("topology.node.switch.mapping.impl", 
        CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY),
      new DeprecationDelta("dfs.df.interval", 
        CommonConfigurationKeys.FS_DF_INTERVAL_KEY),
      new DeprecationDelta("hadoop.native.lib", 
        CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY),
      new DeprecationDelta("fs.default.name", 
        CommonConfigurationKeys.FS_DEFAULT_NAME_KEY),
      new DeprecationDelta("dfs.umaskmode",
        CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY),
      new DeprecationDelta("dfs.nfs.exports.allowed.hosts",
          CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY)
    };

  /**
   * The global DeprecationContext.
   */
  private static AtomicReference<DeprecationContext> deprecationContext =
      new AtomicReference<DeprecationContext>(
          new DeprecationContext(null, defaultDeprecations));

  /**
   * Adds a set of deprecated keys to the global deprecations.
   *
   * This method is lockless.  It works by means of creating a new
   * DeprecationContext based on the old one, and then atomically swapping in
   * the new context.  If someone else updated the context in between us reading
   * the old context and swapping in the new one, we try again until we win the
   * race.
   *
   * @param deltas   The deprecations to add.
   */
  public static void addDeprecations(DeprecationDelta[] deltas) {
    DeprecationContext prev, next;
    do {
      prev = deprecationContext.get();
      next = new DeprecationContext(prev, deltas);
    } while (!deprecationContext.compareAndSet(prev, next));
  }

  /**
   * Adds the deprecated key to the global deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   *
   * If you have multiple deprecation entries to add, it is more efficient to
   * use #addDeprecations(DeprecationDelta[] deltas) instead.
   * 
   * @param key
   * @param newKeys
   * @param customMessage
   * @deprecated use {@link #addDeprecation(String key, String newKey,
      String customMessage)} instead
   */
  @Deprecated
  public static void addDeprecation(String key, String[] newKeys,
      String customMessage) {
    addDeprecations(new DeprecationDelta[] {
      new DeprecationDelta(key, newKeys, customMessage)
    });
  }

  /**
   * Adds the deprecated key to the global deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If you have multiple deprecation entries to add, it is more efficient to
   * use #addDeprecations(DeprecationDelta[] deltas) instead.
   *
   * @param key
   * @param newKey
   * @param customMessage
   */
  public static void addDeprecation(String key, String newKey,
	      String customMessage) {
	  addDeprecation(key, new String[] {newKey}, customMessage);
  }

  /**
   * Adds the deprecated key to the global deprecation map when no custom
   * message is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   * 
   * If you have multiple deprecation entries to add, it is more efficient to
   * use #addDeprecations(DeprecationDelta[] deltas) instead.
   *
   * @param key Key that is to be deprecated
   * @param newKeys list of keys that take up the values of deprecated key
   * @deprecated use {@link #addDeprecation(String key, String newKey)} instead
   */
  @Deprecated
  public static void addDeprecation(String key, String[] newKeys) {
    addDeprecation(key, newKeys, null);
  }
  
  /**
   * Adds the deprecated key to the global deprecation map when no custom
   * message is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If you have multiple deprecation entries to add, it is more efficient to
   * use #addDeprecations(DeprecationDelta[] deltas) instead.
   *
   * @param key Key that is to be deprecated
   * @param newKey key that takes up the value of deprecated key
   */
  public static void addDeprecation(String key, String newKey) {
    addDeprecation(key, new String[] {newKey}, null);
  }
  
  /**
   * checks whether the given <code>key</code> is deprecated.
   * 
   * @param key the parameter which is to be checked for deprecation
   * @return <code>true</code> if the key is deprecated and 
   *         <code>false</code> otherwise.
   */
  public static boolean isDeprecated(String key) {
    return deprecationContext.get().getDeprecatedKeyMap().containsKey(key);
  }

  /**
   * Sets all deprecated properties that are not currently set but have a
   * corresponding new property that is set. Useful for iterating the
   * properties when all deprecated properties for currently set properties
   * need to be present.
   */
  public void setDeprecatedProperties() {
    DeprecationContext deprecations = deprecationContext.get();
    Properties props = getProps();
    Properties overlay = getOverlay();
    for (Map.Entry<String, DeprecatedKeyInfo> entry :
        deprecations.getDeprecatedKeyMap().entrySet()) {
      String depKey = entry.getKey();
      if (!overlay.contains(depKey)) {
        for (String newKey : entry.getValue().newKeys) {
          String val = overlay.getProperty(newKey);
          if (val != null) {
            props.setProperty(depKey, val);
            overlay.setProperty(depKey, val);
            break;
          }
        }
      }
    }
  }

  /**
   * Checks for the presence of the property <code>name</code> in the
   * deprecation map. Returns the first of the list of new keys if present
   * in the deprecation map or the <code>name</code> itself. If the property
   * is not presently set but the property map contains an entry for the
   * deprecated key, the value of the deprecated key is set as the value for
   * the provided property name.
   *
   * @param name the property name
   * @return the first property in the list of properties mapping
   *         the <code>name</code> or the <code>name</code> itself.
   */
  private String[] handleDeprecation(DeprecationContext deprecations,
      String name) {
    if (null != name) {
      name = name.trim();
    }
    ArrayList<String > names = new ArrayList<String>();
	if (isDeprecated(name)) {
      DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap().get(name);
      warnOnceIfDeprecated(deprecations, name);
      for (String newKey : keyInfo.newKeys) {
        if(newKey != null) {
          names.add(newKey);
        }
      }
    }
    if(names.size() == 0) {
    	names.add(name);
    }
    for(String n : names) {
	  String deprecatedKey = deprecations.getReverseDeprecatedKeyMap().get(n);
	  if (deprecatedKey != null && !getOverlay().containsKey(n) &&
	      getOverlay().containsKey(deprecatedKey)) {
	    getProps().setProperty(n, getOverlay().getProperty(deprecatedKey));
	    getOverlay().setProperty(n, getOverlay().getProperty(deprecatedKey));
	  }
    }
    return names.toArray(new String[names.size()]);
  }
 
  private void handleDeprecation() {
    LOG.debug("Handling deprecation for all properties in config...");
    DeprecationContext deprecations = deprecationContext.get();
    Set<Object> keys = new HashSet<Object>();
    keys.addAll(getProps().keySet());
    for (Object item: keys) {
      LOG.debug("Handling deprecation for " + (String)item);
      handleDeprecation(deprecations, (String)item);
    }
  }
 
  static{
    //print deprecation warning if hadoop-site.xml is found in classpath
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    if(cL.getResource("hadoop-site.xml")!=null) {
      LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. " +
          "Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
          + "mapred-site.xml and hdfs-site.xml to override properties of " +
          "core-default-shaded.xml, mapred-default.xml and hdfs-default.xml " +
          "respectively");
    }
    addDefaultResource("core-default-shaded.xml");
    addDefaultResource("core-site.xml");
  }
  
  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }
  
  /** A new configuration. */
  public Configuration() {
    this(true);
  }

  /** A new configuration where the behavior of reading from the default 
   * resources can be turned off.
   * 
   * If the parameter {@code loadDefaults} is false, the new instance
   * will not load resources from the default files. 
   * @param loadDefaults specifies whether to load from the default files
   */
  public Configuration(boolean loadDefaults) {
    this.loadDefaults = loadDefaults;
    updatingResource = new ConcurrentHashMap<String, String[]>();
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
  }
  
  /** 
   * A new configuration with the same settings cloned from another.
   * 
   * @param other the configuration from which to clone settings.
   */
  @SuppressWarnings("unchecked")
  public Configuration(Configuration other) {
   this.resources = (ArrayList<Resource>) other.resources.clone();
   synchronized(other) {
     if (other.properties != null) {
       this.properties = (Properties)other.properties.clone();
     }

     if (other.overlay!=null) {
       this.overlay = (Properties)other.overlay.clone();
     }

     this.updatingResource = new ConcurrentHashMap<String, String[]>(
         other.updatingResource);
     this.finalParameters = Collections.newSetFromMap(
         new ConcurrentHashMap<String, Boolean>());
     this.finalParameters.addAll(other.finalParameters);
   }
   
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
    this.classLoader = other.classLoader;
    this.loadDefaults = other.loadDefaults;
    setQuietMode(other.getQuietMode());
  }
  
  /**
   * Add a default resource. Resources are loaded in the order of the resources 
   * added.
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(String name) {
    if(!defaultResources.contains(name)) {
      defaultResources.add(name);
      for(Configuration conf : REGISTRY.keySet()) {
        if(conf.loadDefaults) {
          conf.reloadConfiguration();
        }
      }
    }
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param name resource to be added, the classpath is examined for a file 
   *             with that name.
   */
  public void addResource(String name) {
    addResourceObject(new Resource(name));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param url url of the resource to be added, the local filesystem is 
   *            examined directly to find the resource, without referring to 
   *            the classpath.
   */
  public void addResource(URL url) {
    addResourceObject(new Resource(url));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param file file-path of resource to be added, the local filesystem is
   *             examined directly to find the resource, without referring to 
   *             the classpath.
   */
  public void addResource(Path file) {
    addResourceObject(new Resource(file));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * WARNING: The contents of the InputStream will be cached, by this method. 
   * So use this sparingly because it does increase the memory consumption.
   * 
   * @param in InputStream to deserialize the object from. In will be read from
   * when a get or set is called next.  After it is read the stream will be
   * closed. 
   */
  public void addResource(InputStream in) {
    addResourceObject(new Resource(in));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param in InputStream to deserialize the object from.
   * @param name the name of the resource because InputStream.toString is not
   * very descriptive some times.  
   */
  public void addResource(InputStream in, String name) {
    addResourceObject(new Resource(in, name));
  }
  
  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param conf Configuration object from which to load properties
   */
  public void addResource(Configuration conf) {
    addResourceObject(new Resource(conf.getProps()));
  }

  
  
  /**
   * Reload configuration from previously added resources.
   *
   * This method will clear all the configuration read from the added 
   * resources, and final parameters. This will make the resources to 
   * be read again before accessing the values. Values that are added
   * via set methods will overlay values read from the resources.
   */
  public synchronized void reloadConfiguration() {
    properties = null;                            // trigger reload
    finalParameters.clear();                      // clear site-limits
  }
  
  private synchronized void addResourceObject(Resource resource) {
    resources.add(resource);                      // add to resources
    reloadConfiguration();
  }

  private static final int MAX_SUBST = 20;

  private static final int SUB_START_IDX = 0;
  private static final int SUB_END_IDX = SUB_START_IDX + 1;

  /**
   * This is a manual implementation of the following regex
   * "\\$\\{[^\\}\\$\u0020]+\\}". It can be 15x more efficient than
   * a regex matcher as demonstrated by HADOOP-11506. This is noticeable with
   * Hadoop apps building on the assumption Configuration#get is an O(1)
   * hash table lookup, especially when the eval is a long string.
   *
   * @param eval a string that may contain variables requiring expansion.
   * @return a 2-element int array res such that
   * eval.substring(res[0], res[1]) is "var" for the left-most occurrence of
   * ${var} in eval. If no variable is found -1, -1 is returned.
   */
  private static int[] findSubVariable(String eval) {
    int[] result = {-1, -1};

    int matchStart;
    int leftBrace;

    // scanning for a brace first because it's less frequent than $
    // that can occur in nested class names
    //
    match_loop:
    for (matchStart = 1, leftBrace = eval.indexOf('{', matchStart);
         // minimum left brace position (follows '$')
         leftBrace > 0
         // right brace of a smallest valid expression "${c}"
         && leftBrace + "{c".length() < eval.length();
         leftBrace = eval.indexOf('{', matchStart)) {
      int matchedLen = 0;
      if (eval.charAt(leftBrace - 1) == '$') {
        int subStart = leftBrace + 1; // after '{'
        for (int i = subStart; i < eval.length(); i++) {
          switch (eval.charAt(i)) {
            case '}':
              if (matchedLen > 0) { // match
                result[SUB_START_IDX] = subStart;
                result[SUB_END_IDX] = subStart + matchedLen;
                break match_loop;
              }
              // fall through to skip 1 char
            case ' ':
            case '$':
              matchStart = i + 1;
              continue match_loop;
            default:
              matchedLen++;
          }
        }
        // scanned from "${"  to the end of eval, and no reset via ' ', '$':
        //    no match!
        break match_loop;
      } else {
        // not a start of a variable
        //
        matchStart = leftBrace + 1;
      }
    }
    return result;
  }

  /**
   * Attempts to repeatedly expand the value {@code expr} by replacing the
   * left-most substring of the form "${var}" in the following precedence order
   * <ol>
   *   <li>by the value of the Java system property "var" if defined</li>
   *   <li>by the value of the configuration key "var" if defined</li>
   * </ol>
   *
   * If var is unbounded the current state of expansion "prefix${var}suffix" is
   * returned.
   *
   * @param expr the literal value of a config key
   * @return null if expr is null, otherwise the value resulting from expanding
   * expr using the algorithm above.
   * @throws IllegalArgumentException when more than
   * {@link Configuration#MAX_SUBST} replacements are required
   */
  private String substituteVars(String expr) {
    if (expr == null) {
      return null;
    }
    String eval = expr;
    for (int s = 0; s < MAX_SUBST; s++) {
      final int[] varBounds = findSubVariable(eval);
      if (varBounds[SUB_START_IDX] == -1) {
        return eval;
      }
      final String var = eval.substring(varBounds[SUB_START_IDX],
          varBounds[SUB_END_IDX]);
      String val = null;
      try {
        val = System.getProperty(var);
      } catch(SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      final int dollar = varBounds[SUB_START_IDX] - "${".length();
      final int afterRightBrace = varBounds[SUB_END_IDX] + "}".length();
      // substitute
      eval = eval.substring(0, dollar)
             + val
             + eval.substring(afterRightBrace);
    }
    throw new IllegalStateException("Variable substitution depth too large: " 
                                    + MAX_SUBST + " " + expr);
  }
  
  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists. If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null.
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name, will be trimmed before get value.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  public String get(String name) {
    String[] names = handleDeprecation(deprecationContext.get(), name);
    String result = null;
    for(String n : names) {
      result = substituteVars(getProps().getProperty(n));
    }
    return result;
  }

  /**
   * Set Configuration to allow keys without values during setup.  Intended
   * for use during testing.
   *
   * @param val If true, will allow Configuration to store keys without values
   */
  @VisibleForTesting
  public void setAllowNullValueProperties( boolean val ) {
    this.allowNullValueProperties = val;
  }

  /**
   * Return existence of the <code>name</code> property, but only for
   * names which have no valid value, usually non-existent or commented
   * out in XML.
   *
   * @param name the property name
   * @return true if the property <code>name</code> exists without value
   */
  @VisibleForTesting
  public boolean onlyKeyExists(String name) {
    String[] names = handleDeprecation(deprecationContext.get(), name);
    for(String n : names) {
      if ( getProps().getProperty(n,DEFAULT_STRING_CHECK)
               .equals(DEFAULT_STRING_CHECK) ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>null</code> if no such property exists. 
   * If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  public String getTrimmed(String name) {
    String value = get(name);
    
    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }
  
  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>defaultValue</code> if no such property exists. 
   * See @{Configuration#getTrimmed} for more details.
   * 
   * @param name          the property name.
   * @param defaultValue  the property default value.
   * @return              the value of the <code>name</code> or defaultValue
   *                      if it is not set.
   */
  public String getTrimmed(String name, String defaultValue) {
    String ret = getTrimmed(name);
    return ret == null ? defaultValue : ret;
  }

  /**
   * Get the value of the <code>name</code> property, without doing
   * <a href="#VariableExpansion">variable expansion</a>.If the key is 
   * deprecated, it returns the value of the first key which replaces 
   * the deprecated key and is not null.
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> property or 
   *         its replacing property and null if no such property exists.
   */
  public String getRaw(String name) {
    String[] names = handleDeprecation(deprecationContext.get(), name);
    String result = null;
    for(String n : names) {
      result = getProps().getProperty(n);
    }
    return result;
  }

  /**
   * Returns alternative names (non-deprecated keys or previously-set deprecated keys)
   * for a given non-deprecated key.
   * If the given key is deprecated, return null.
   *
   * @param name property name.
   * @return alternative names.
   */
  private String[] getAlternativeNames(String name) {
    String altNames[] = null;
    DeprecatedKeyInfo keyInfo = null;
    DeprecationContext cur = deprecationContext.get();
    String depKey = cur.getReverseDeprecatedKeyMap().get(name);
    if(depKey != null) {
      keyInfo = cur.getDeprecatedKeyMap().get(depKey);
      if(keyInfo.newKeys.length > 0) {
        if(getProps().containsKey(depKey)) {
          //if deprecated key is previously set explicitly
          List<String> list = new ArrayList<String>();
          list.addAll(Arrays.asList(keyInfo.newKeys));
          list.add(depKey);
          altNames = list.toArray(new String[list.size()]);
        }
        else {
          altNames = keyInfo.newKeys;
        }
      }
    }
    return altNames;
  }

  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names. Name will be trimmed before put into
   * configuration.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void set(String name, String value) {
    set(name, value, null);
  }
  
  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated, it also sets the <code>value</code> to
   * the keys that replace the deprecated key. Name will be trimmed before put
   * into configuration.
   *
   * @param name property name.
   * @param value property value.
   * @param source the place that this configuration value came from 
   * (For debugging).
   * @throws IllegalArgumentException when the value or name is null.
   */
  public void set(String name, String value, String source) {
    Preconditions.checkArgument(
        name != null,
        "Property name must not be null");
    Preconditions.checkArgument(
        value != null,
        "The value of property " + name + " must not be null");
    name = name.trim();
    DeprecationContext deprecations = deprecationContext.get();
    if (deprecations.getDeprecatedKeyMap().isEmpty()) {
      getProps();
    }
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
    String newSource = (source == null ? "programatically" : source);

    if (!isDeprecated(name)) {
      updatingResource.put(name, new String[] {newSource});
      String[] altNames = getAlternativeNames(name);
      if(altNames != null) {
        for(String n: altNames) {
          if(!n.equals(name)) {
            getOverlay().setProperty(n, value);
            getProps().setProperty(n, value);
            updatingResource.put(n, new String[] {newSource});
          }
        }
      }
    }
    else {
      String[] names = handleDeprecation(deprecationContext.get(), name);
      String altSource = "because " + name + " is deprecated";
      for(String n : names) {
        getOverlay().setProperty(n, value);
        getProps().setProperty(n, value);
        updatingResource.put(n, new String[] {altSource});
      }
    }
  }

  private void warnOnceIfDeprecated(DeprecationContext deprecations, String name) {
    DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap().get(name);
    if (keyInfo != null && !keyInfo.getAndSetAccessed()) {
      LOG_DEPRECATION.info(keyInfo.getWarningMessage(name));
    }
  }

  /**
   * Unset a previously set property.
   */
  public synchronized void unset(String name) {
    String[] names = null;
    if (!isDeprecated(name)) {
      names = getAlternativeNames(name);
      if(names == null) {
    	  names = new String[]{name};
      }
    }
    else {
      names = handleDeprecation(deprecationContext.get(), name);
    }

    for(String n: names) {
      getOverlay().remove(n);
      getProps().remove(n);
    }
  }

  /**
   * Sets a property if it is currently unset.
   * @param name the property name
   * @param value the new value
   */
  public synchronized void setIfUnset(String name, String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }
  
  private synchronized Properties getOverlay() {
    if (overlay==null){
      overlay=new Properties();
    }
    return overlay;
  }

  /** 
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.
   * 
   * @param name property name, will be trimmed before get value.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property 
   *         doesn't exist.                    
   */
  public String get(String name, String defaultValue) {
    String[] names = handleDeprecation(deprecationContext.get(), name);
    String result = null;
    for(String n : names) {
      result = substituteVars(getProps().getProperty(n, defaultValue));
    }
    return result;
  }

  /** 
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *   
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>int</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>, 
   *         or <code>defaultValue</code>. 
   */
  public int getInt(String name, int defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }
  
  /**
   * Get the value of the <code>name</code> property as a set of comma-delimited
   * <code>int</code> values.
   * 
   * If no such property exists, an empty array is returned.
   * 
   * @param name property name
   * @return property value interpreted as an array of comma-delimited
   *         <code>int</code> values
   */
  public int[] getInts(String name) {
    String[] strings = getTrimmedStrings(name);
    int[] ints = new int[strings.length];
    for (int i = 0; i < strings.length; i++) {
      ints[i] = Integer.parseInt(strings[i]);
    }
    return ints;
  }

  /** 
   * Set the value of the <code>name</code> property to an <code>int</code>.
   * 
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }


  /** 
   * Get the value of the <code>name</code> property as a <code>long</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>long</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>, 
   *         or <code>defaultValue</code>. 
   */
  public long getLong(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists, the provided default
   * value is returned, or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public long getLongBytes(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
  }

  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }
  
  /** 
   * Set the value of the <code>name</code> property to a <code>long</code>.
   * 
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>float</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>float</code>, 
   *         or <code>defaultValue</code>. 
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Float.parseFloat(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void setFloat(String name, float value) {
    set(name,Float.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>double</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>double</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>double</code>, 
   *         or <code>defaultValue</code>. 
   */
  public double getDouble(String name, double defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Double.parseDouble(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>double</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void setDouble(String name, double value) {
    set(name,Double.toString(value));
  }
 
  /** 
   * Get the value of the <code>name</code> property as a <code>boolean</code>.  
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>, 
   *         or <code>defaultValue</code>. 
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = getTrimmed(name);
    if (null == valueString || valueString.isEmpty()) {
      return defaultValue;
    }

    if (StringUtils.equalsIgnoreCase("true", valueString))
      return true;
    else if (StringUtils.equalsIgnoreCase("false", valueString))
      return false;
    else return defaultValue;
  }

  /** 
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   * 
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /**
   * Set the given property, if it is currently unset.
   * @param name property name
   * @param value new value
   */
  public void setBooleanIfUnset(String name, boolean value) {
    setIfUnset(name, Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This
   * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
   * @param name property name
   * @param value new value
   */
  public <T extends Enum<T>> void setEnum(String name, T value) {
    set(name, value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * Note that the returned value is trimmed by this method.
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    final String val = getTrimmed(name);
    return null == val
      ? defaultValue
      : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  enum ParsedTimeDuration {
    NS {
      TimeUnit unit() { return TimeUnit.NANOSECONDS; }
      String suffix() { return "ns"; }
    },
    US {
      TimeUnit unit() { return TimeUnit.MICROSECONDS; }
      String suffix() { return "us"; }
    },
    MS {
      TimeUnit unit() { return TimeUnit.MILLISECONDS; }
      String suffix() { return "ms"; }
    },
    S {
      TimeUnit unit() { return TimeUnit.SECONDS; }
      String suffix() { return "s"; }
    },
    M {
      TimeUnit unit() { return TimeUnit.MINUTES; }
      String suffix() { return "m"; }
    },
    H {
      TimeUnit unit() { return TimeUnit.HOURS; }
      String suffix() { return "h"; }
    },
    D {
      TimeUnit unit() { return TimeUnit.DAYS; }
      String suffix() { return "d"; }
    };
    abstract TimeUnit unit();
    abstract String suffix();
    static ParsedTimeDuration unitFor(String s) {
      for (ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }
    static ParsedTimeDuration unitFor(TimeUnit unit) {
      for (ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }

  /**
   * Set the value of <code>name</code> to the given time duration. This
   * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
   * @param name Property name
   * @param value Time duration
   * @param unit Unit of time
   */
  public void setTimeDuration(String name, long value, TimeUnit unit) {
    set(name, value + ParsedTimeDuration.unitFor(unit).suffix());
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists.
   * @param unit Unit to convert the stored property, if it exists.
   * @throws NumberFormatException If the property stripped of its unit is not
   *         a number
   */
  public long getTimeDuration(String name, long defaultValue, TimeUnit unit) {
    String vStr = get(name);
    if (null == vStr) {
      return defaultValue;
    }
    vStr = vStr.trim();
    return getTimeDurationHelper(name, vStr, unit);
  }

  private long getTimeDurationHelper(String name, String vStr, TimeUnit unit) {
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
      vUnit = ParsedTimeDuration.unitFor(unit);
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }
    return unit.convert(Long.parseLong(vStr), vUnit.unit());
  }

  public long[] getTimeDurations(String name, TimeUnit unit) {
    String[] strings = getTrimmedStrings(name);
    long[] durations = new long[strings.length];
    for (int i = 0; i < strings.length; i++) {
      durations[i] = getTimeDurationHelper(name, strings[i], unit);
    }
    return durations;
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   * Note that the returned value is NOT trimmed by this method.
   *
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public Pattern getPattern(String name, Pattern defaultValue) {
    String valString = get(name);
    if (null == valString || valString.isEmpty()) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" +
               name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>.
   * If the pattern is passed as null, sets the empty pattern which results in
   * further calls to getPattern(...) returning the default value.
   *
   * @param name property name
   * @param pattern new value
   */
  public void setPattern(String name, Pattern pattern) {
    assert pattern != null : "Pattern cannot be null";
    set(name, pattern.pattern());
  }

  /**
   * Gets information about why a property was set.  Typically this is the 
   * path to the resource objects (file, URL, etc.) the property came from, but
   * it can also indicate that it was set programatically, or because of the
   * command line.
   *
   * @param name - The property name to get the source of.
   * @return null - If the property or its source wasn't found. Otherwise, 
   * returns a list of the sources of the resource.  The older sources are
   * the first ones in the list.  So for example if a configuration is set from
   * the command line, and then written out to a file that is read back in the
   * first entry would indicate that it was set from the command line, while
   * the second one would indicate the file that the new configuration was read
   * in from.
   */
  @InterfaceStability.Unstable
  public synchronized String[] getPropertySources(String name) {
    if (properties == null) {
      // If properties is null, it means a resource was newly added
      // but the props were cleared so as to load it upon future
      // requests. So lets force a load by asking a properties list.
      getProps();
    }
    // Return a null right away if our properties still
    // haven't loaded or the resource mapping isn't defined
    if (properties == null || updatingResource == null) {
      return null;
    } else {
      String[] source = updatingResource.get(name);
      if(source == null) {
        return null;
      } else {
        return Arrays.copyOf(source, source.length);
      }
    }
  }

  /**
   * A class that represents a set of positive integer ranges. It parses
   * strings of the form: "2-3,5,7-" where ranges are separated by comma and
   * the lower/upper bounds are separated by dash. Either the lower or upper
   * bound may be omitted meaning all values up to or over. So the string
   * above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges implements Iterable<Integer>{
    private static class Range {
      int start;
      int end;
    }

    private static class RangeNumberIterator implements Iterator<Integer> {
      Iterator<Range> internal;
      int at;
      int end;

      public RangeNumberIterator(List<Range> ranges) {
        if (ranges != null) {
          internal = ranges.iterator();
        }
        at = -1;
        end = -2;
      }

      @Override
      public boolean hasNext() {
        if (at <= end) {
          return true;
        } else if (internal != null){
          return internal.hasNext();
        }
        return false;
      }

      @Override
      public Integer next() {
        if (at <= end) {
          at++;
          return at - 1;
        } else if (internal != null){
          Range found = internal.next();
          if (found != null) {
            at = found.start;
            end = found.end;
            at++;
            return at - 1;
          }
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };

    List<Range> ranges = new ArrayList<Range>();

    public IntegerRanges() {
    }

    public IntegerRanges(String newValue) {
      StringTokenizer itr = new StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        String rng = itr.nextToken().trim();
        String[] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new IllegalArgumentException("integer range badly formed: " +
                                             rng);
        }
        Range r = new Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new IllegalArgumentException("IntegerRange from " + r.start +
                                             " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static int convertToInt(String value, int defaultValue) {
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public boolean isIncluded(int value) {
      for(Range r: ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if there are no values in this range, else false.
     */
    public boolean isEmpty() {
      return ranges == null || ranges.isEmpty();
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      boolean first = true;
      for(Range r: ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }

    @Override
    public Iterator<Integer> iterator() {
      return new RangeNumberIterator(ranges);
    }

  }

  /**
   * Parse the given attribute as a set of integer ranges
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s.  
   * If no such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   * 
   * @param name property name.
   * @return property value as a collection of <code>String</code>s. 
   */
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    return StringUtils.getStringCollection(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then <code>null</code> is returned.
   * 
   * @param name property name.
   * @return property value as an array of <code>String</code>s, 
   *         or <code>null</code>. 
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s, 
   *         or default value. 
   */
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.  
   * If no such property is specified then empty <code>Collection</code> is returned.
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code> 
   */
  public Collection<String> getTrimmedStringCollection(String name) {
    String valueString = get(name);
    if (null == valueString) {
      Collection<String> empty = new ArrayList<String>();
      return empty;
    }
    return StringUtils.getTrimmedStringCollection(valueString);
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then an empty array is returned.
   * 
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or empty array. 
   */
  public String[] getTrimmedStrings(String name) {
    String valueString = get(name);
    return StringUtils.getTrimmedStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or default value. 
   */
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return StringUtils.getTrimmedStrings(valueString);
    }
  }

  /** 
   * Set the array of string values for the <code>name</code> property as 
   * as comma delimited values.  
   * 
   * @param name property name.
   * @param values The values
   */
  public void setStrings(String name, String... values) {
    set(name, StringUtils.arrayToString(values));
  }

  /**
   * Get the value for a known password configuration element.
   * In order to enable the elimination of clear text passwords in config,
   * this method attempts to resolve the property name as an alias through
   * the CredentialProvider API and conditionally falls back to config.
   * @param name property name
   * @return password
   */
  public char[] getPassword(String name) throws IOException {
    char[] pass = null;

    pass = getPasswordFromCredentialProviders(name);

    if (pass == null) {
      pass = getPasswordFromConfig(name);
    }

    return pass;
  }

  /**
   * Try and resolve the provided element name as a credential provider
   * alias.
   * @param name alias of the provisioned credential
   * @return password or null if not found
   * @throws IOException
   */
  protected char[] getPasswordFromCredentialProviders(String name)
      throws IOException {
    char[] pass = null;
    try {
      List<CredentialProvider> providers =
          CredentialProviderFactory.getProviders(this);

      if (providers != null) {
        for (CredentialProvider provider : providers) {
          try {
            CredentialEntry entry = provider.getCredentialEntry(name);
            if (entry != null) {
              pass = entry.getCredential();
              break;
            }
          }
          catch (IOException ioe) {
            throw new IOException("Can't get key " + name + " from key provider" +
            		"of type: " + provider.getClass().getName() + ".", ioe);
          }
        }
      }
    }
    catch (IOException ioe) {
      throw new IOException("Configuration problem with provider path.", ioe);
    }

    return pass;
  }

  /**
   * Fallback to clear text passwords in configuration.
   * @param name
   * @return clear text password or null
   */
  protected char[] getPasswordFromConfig(String name) {
    char[] pass = null;
    if (getBoolean(CredentialProvider.CLEAR_TEXT_FALLBACK, true)) {
      String passStr = get(name);
      if (passStr != null) {
        pass = passStr.toCharArray();
      }
    }
    return pass;
  }

  /**
   * Get the socket address for <code>hostProperty</code> as a
   * <code>InetSocketAddress</code>. If <code>hostProperty</code> is
   * <code>null</code>, <code>addressProperty</code> will be used. This
   * is useful for cases where we want to differentiate between host
   * bind address and address clients should use to establish connection.
   *
   * @param hostProperty bind host property name.
   * @param addressProperty address property name.
   * @param defaultAddressValue the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public InetSocketAddress getSocketAddr(
      String hostProperty,
      String addressProperty,
      String defaultAddressValue,
      int defaultPort) {

    InetSocketAddress bindAddr = getSocketAddr(
      addressProperty, defaultAddressValue, defaultPort);

    final String host = get(hostProperty);

    if (host == null || host.isEmpty()) {
      return bindAddr;
    }

    return NetUtils.createSocketAddr(
        host, bindAddr.getPort(), hostProperty);
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public InetSocketAddress getSocketAddr(
      String name, String defaultAddress, int defaultPort) {
    final String address = getTrimmed(name, defaultAddress);
    return NetUtils.createSocketAddr(address, defaultPort, name);
  }

  /**
   * Set the socket address for the <code>name</code> property as
   * a <code>host:port</code>.
   */
  public void setSocketAddr(String name, InetSocketAddress addr) {
    set(name, NetUtils.getHostPortString(addr));
  }

  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address. If the host and address
   * properties are configured the host component of the address will be combined
   * with the port component of the addr to generate the address.  This is to allow
   * optional control over which host name is used in multi-home bind-host
   * cases where a host can have multiple names
   * @param hostProperty the bind-host configuration name
   * @param addressProperty the service address configuration name
   * @param defaultAddressValue the service default address configuration value
   * @param addr InetSocketAddress of the service listener
   * @return InetSocketAddress for clients to connect
   */
  public InetSocketAddress updateConnectAddr(
      String hostProperty,
      String addressProperty,
      String defaultAddressValue,
      InetSocketAddress addr) {

    final String host = get(hostProperty);
    final String connectHostPort = getTrimmed(addressProperty, defaultAddressValue);

    if (host == null || host.isEmpty() || connectHostPort == null || connectHostPort.isEmpty()) {
      //not our case, fall back to original logic
      return updateConnectAddr(addressProperty, addr);
    }

    final String connectHost = connectHostPort.split(":")[0];
    // Create connect address using client address hostname and server port.
    return updateConnectAddr(addressProperty, NetUtils.createSocketAddrForHost(
        connectHost, addr.getPort()));
  }
  
  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address.
   * @param name property name.
   * @param addr InetSocketAddress of a listener to store in the given property
   * @return InetSocketAddress for clients to connect
   */
  public InetSocketAddress updateConnectAddr(String name,
                                             InetSocketAddress addr) {
    final InetSocketAddress connectAddr = NetUtils.getConnectAddress(addr);
    setSocketAddr(name, connectAddr);
    return connectAddr;
  }
  
  /**
   * Load a class by name.
   * 
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    Class<?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }
  
  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   * 
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  public Class<?> getClassByNameOrNull(String name) {
    Map<String, WeakReference<Class<?>>> map;
    
    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(
          new WeakHashMap<String, WeakReference<Class<?>>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    Class<?> clazz = null;
    WeakReference<Class<?>> ref = map.get(name); 
    if (ref != null) {
       clazz = ref.get();
    }
     
    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, classLoader);
      } catch (ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, new WeakReference<Class<?>>(NEGATIVE_CACHE_SENTINEL));
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, new WeakReference<Class<?>>(clazz));
      return clazz;
    } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
  }

  /** 
   * Get the value of the <code>name</code> property
   * as an array of <code>Class</code>.
   * The value of the property specifies a list of comma separated class names.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>, 
   *         or <code>defaultValue</code>. 
   */
  public Class<?>[] getClasses(String name, Class<?> ... defaultValue) {
    String[] classnames = getTrimmedStrings(name);
    if (classnames == null)
      return defaultValue;
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for(int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>. 
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   *   
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * An exception is thrown if the returned class does not implement the named
   * interface. 
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>.
   */
  public <U> Class<? extends U> getClass(String name, 
                                         Class<? extends U> defaultValue, 
                                         Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass+" not "+xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>List</code>
   * of objects implementing the interface specified by <code>xface</code>.
   * 
   * An exception is thrown if any of the classes does not exist, or if it does
   * not implement the named interface.
   * 
   * @param name the property name.
   * @param xface the interface implemented by the classes named by
   *        <code>name</code>.
   * @return a <code>List</code> of objects implementing <code>xface</code>.
   */
  @SuppressWarnings("unchecked")
  public <U> List<U> getInstances(String name, Class<U> xface) {
    List<U> ret = new ArrayList<U>();
    Class<?>[] classes = getClasses(name);
    for (Class<?> cl: classes) {
      if (!xface.isAssignableFrom(cl)) {
        throw new RuntimeException(cl + " does not implement " + xface);
      }
      ret.add((U)ReflectionUtils.newInstance(cl, this));
    }
    return ret;
  }

  /** 
   * Set the value of the <code>name</code> property to the name of a 
   * <code>theClass</code> implementing the given interface <code>xface</code>.
   * 
   * An exception is thrown if <code>theClass</code> does not implement the 
   * interface <code>xface</code>. 
   * 
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(name, theClass.getName());
  }

  /** 
   * Get a local file under a directory named by <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public Path getLocalPath(String dirsProp, String path)
    throws IOException {
    String[] dirs = getTrimmedStrings(dirsProp);
    int hashCode = path.hashCode();
    FileSystem fs = FileSystem.getLocal(this);
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      Path file = new Path(dirs[index], path);
      Path dir = file.getParent();
      if (fs.mkdirs(dir) || fs.exists(dir)) {
        return file;
      }
    }
    LOG.warn("Could not make " + path + 
             " in local directories from " + dirsProp);
    for(int i=0; i < dirs.length; i++) {
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get a local file name under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public File getFile(String dirsProp, String path)
    throws IOException {
    String[] dirs = getTrimmedStrings(dirsProp);
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path);
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get the {@link URL} for the named resource.
   * 
   * @param name resource name.
   * @return the url for the named resource.
   */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }
  
  /** 
   * Get an input stream attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return an input stream attached to the resource.
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /** 
   * Get a {@link Reader} attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return a reader attached to the resource.
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream(), Charsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Get the set of parameters marked final.
   *
   * @return final parameter set.
   */
  public Set<String> getFinalParameters() {
    Set<String> setFinalParams = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    setFinalParams.addAll(finalParameters);
    return setFinalParams;
  }

  protected synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      Map<String, String[]> backup =
          new ConcurrentHashMap<String, String[]>(updatingResource);
      loadResources(properties, resources, quietmode);

      if (overlay != null) {
        properties.putAll(overlay);
        for (Map.Entry<Object,Object> item: overlay.entrySet()) {
          String key = (String)item.getKey();
          String[] source = backup.get(key);
          if(source != null) {
            updatingResource.put(key, source);
          }
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   *
   * @return number of keys in the configuration.
   */
  public int size() {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear() {
    getProps().clear();
    getOverlay().clear();
  }

  /**
   * Get an {@link Iterator} to go through the list of <code>String</code> 
   * key-value pairs in the configuration.
   * 
   * @return an iterator over the entries.
   */
  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    // Get a copy of just the string to string pairs. After the old object
    // methods that allow non-strings to be put into configurations are removed,
    // we could replace properties with a Map<String,String> and get rid of this
    // code.
    Map<String,String> result = new HashMap<String,String>();
    for(Map.Entry<Object,Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof String &&
          item.getValue() instanceof String) {
          result.put((String) item.getKey(), (String) item.getValue());
      }
    }
    return result.entrySet().iterator();
  }

  /**
   * Constructs a mapping of configuration and includes all properties that
   * start with the specified configuration prefix.  Property names in the
   * mapping are trimmed to remove the configuration prefix.
   *
   * @param confPrefix configuration prefix
   * @return mapping of configuration properties with prefix stripped
   */
  public Map<String, String> getPropsWithPrefix(String confPrefix) {
    Map<String, String> configMap = new HashMap<>();
    for (Map.Entry<String, String> entry : this) {
      String name = entry.getKey();
      if (name.startsWith(confPrefix)) {
        String value = this.get(name);
        name = name.substring(confPrefix.length());
        configMap.put(name, value);
      }
    }
    return configMap;
  }

  private Document parse(DocumentBuilder builder, URL url)
      throws IOException, SAXException {
    if (!quietmode) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("parsing URL " + url);
      }
    }
    if (url == null) {
      return null;
    }

    URLConnection connection = url.openConnection();
    if (connection instanceof JarURLConnection) {
      // Disable caching for JarURLConnection to avoid sharing JarFile
      // with other users.
      connection.setUseCaches(false);
    }
    return parse(builder, connection.getInputStream(), url.toString());
  }

  private Document parse(DocumentBuilder builder, InputStream is,
      String systemId) throws IOException, SAXException {
    if (!quietmode) {
      LOG.debug("parsing input stream " + is);
    }
    if (is == null) {
      return null;
    }
    try {
      return (systemId == null) ? builder.parse(is) : builder.parse(is,
          systemId);
    } finally {
      is.close();
    }
  }

  private void loadResources(Properties properties,
                             ArrayList<Resource> resources,
                             boolean quiet) {
    if(loadDefaults) {
      for (String resource : defaultResources) {
        loadResource(properties, new Resource(resource), quiet);
      }
    
      //support the hadoop-site.xml as a deprecated case
      if(getResource("hadoop-site.xml")!=null) {
        loadResource(properties, new Resource("hadoop-site.xml"), quiet);
      }
    }
    
    for (int i = 0; i < resources.size(); i++) {
      Resource ret = loadResource(properties, resources.get(i), quiet);
      if (ret != null) {
        resources.set(i, ret);
      }
    }
  }
  
  private Resource loadResource(Properties properties, Resource wrapper, boolean quiet) {
    String name = UNKNOWN_RESOURCE;
    try {
      Object resource = wrapper.getResource();
      name = wrapper.getName();
      
      DocumentBuilderFactory docBuilderFactory 
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
          docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser "
                + docBuilderFactory
                + ":" + e,
                e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;
      boolean returnCachedProperties = false;
      
      if (resource instanceof URL) {                  // an URL resource
        doc = parse(builder, (URL)resource);
      } else if (resource instanceof String) {        // a CLASSPATH resource
        URL url = getResource((String)resource);
        doc = parse(builder, url);
      } else if (resource instanceof Path) {          // a file resource
        // Can't use FileSystem API or we get an infinite loop
        // since FileSystem uses Configuration API.  Use java.io.File instead.
        File file = new File(((Path)resource).toUri().getPath())
          .getAbsoluteFile();
        if (file.exists()) {
          if (!quiet) {
            LOG.debug("parsing File " + file);
          }
          doc = parse(builder, new BufferedInputStream(
              new FileInputStream(file)), ((Path)resource).toString());
        }
      } else if (resource instanceof InputStream) {
        doc = parse(builder, (InputStream) resource, null);
        returnCachedProperties = true;
      } else if (resource instanceof Properties) {
        overlay(properties, (Properties)resource);
      } else if (resource instanceof Element) {
        root = (Element)resource;
      }

      if (root == null) {
        if (doc == null) {
          if (quiet) {
            return null;
          }
          throw new RuntimeException(resource + " not found");
        }
        root = doc.getDocumentElement();
      }
      Properties toAddTo = properties;
      if(returnCachedProperties) {
        toAddTo = new Properties();
      }
      if (!"configuration".equals(root.getTagName()))
        LOG.fatal("bad conf file: top-level element not <configuration>");
      NodeList props = root.getChildNodes();
      DeprecationContext deprecations = deprecationContext.get();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element)propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(toAddTo, new Resource(prop, name), quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        boolean finalParameter = false;
        LinkedList<String> source = new LinkedList<String>();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes())
            attr = StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData().trim());
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData());
          if ("final".equals(field.getTagName()) && field.hasChildNodes())
            finalParameter = "true".equals(((Text)field.getFirstChild()).getData());
          if ("source".equals(field.getTagName()) && field.hasChildNodes())
            source.add(StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData()));
        }
        source.add(name);
        
        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          if (deprecations.getDeprecatedKeyMap().containsKey(attr)) {
            DeprecatedKeyInfo keyInfo =
                deprecations.getDeprecatedKeyMap().get(attr);
            keyInfo.clearAccessed();
            for (String key:keyInfo.newKeys) {
              // update new keys with deprecated key's value 
              loadProperty(toAddTo, name, key, value, finalParameter, 
                  source.toArray(new String[source.size()]));
            }
          }
          else {
            loadProperty(toAddTo, name, attr, value, finalParameter, 
                source.toArray(new String[source.size()]));
          }
        }
      }
      
      if (returnCachedProperties) {
        overlay(properties, toAddTo);
        return new Resource(toAddTo, name);
      }
      return null;
    } catch (IOException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new RuntimeException(e);
    } catch (DOMException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new RuntimeException(e);
    } catch (SAXException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new RuntimeException(e);
    } catch (ParserConfigurationException e) {
      LOG.fatal("error parsing conf " + name , e);
      throw new RuntimeException(e);
    }
  }

  private void overlay(Properties to, Properties from) {
    for (Entry<Object, Object> entry: from.entrySet()) {
      to.put(entry.getKey(), entry.getValue());
    }
  }

  private void loadProperty(Properties properties, String name, String attr,
      String value, boolean finalParameter, String[] source) {
    if (value != null || allowNullValueProperties) {
      if (value == null) {
        value = DEFAULT_STRING_CHECK;
      }
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        if(source != null) {
          updatingResource.put(attr, source);
        }
      } else if (!value.equals(properties.getProperty(attr))) {
        LOG.warn(name+":an attempt to override final parameter: "+attr
            +";  Ignoring.");
      }
    }
    if (finalParameter && attr != null) {
      finalParameters.add(attr);
    }
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link OutputStream} using UTF-8 encoding.
   * 
   * @param out the output stream to write to.
   */
  public void writeXml(OutputStream out) throws IOException {
    writeXml(new OutputStreamWriter(out, "UTF-8"));
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link Writer}.
   * 
   * @param out the writer to write to.
   */
  public void writeXml(Writer out) throws IOException {
    Document doc = asXmlDocument();

    try {
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();

      // Important to not hold Configuration log while writing result, since
      // 'out' may be an HDFS stream which needs to lock this configuration
      // from another thread.
      transformer.transform(source, result);
    } catch (TransformerException te) {
      throw new IOException(te);
    }
  }

  /**
   * Return the XML DOM corresponding to this Configuration.
   */
  private synchronized Document asXmlDocument() throws IOException {
    Document doc;
    try {
      doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    } catch (ParserConfigurationException pe) {
      throw new IOException(pe);
    }
    Element conf = doc.createElement("configuration");
    doc.appendChild(conf);
    conf.appendChild(doc.createTextNode("\n"));
    handleDeprecation(); //ensure properties is set and deprecation is handled
    for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
      String name = (String)e.nextElement();
      Object object = properties.get(name);
      String value = null;
      if (object instanceof String) {
        value = (String) object;
      }else {
        continue;
      }
      Element propNode = doc.createElement("property");
      conf.appendChild(propNode);

      Element nameNode = doc.createElement("name");
      nameNode.appendChild(doc.createTextNode(name));
      propNode.appendChild(nameNode);

      Element valueNode = doc.createElement("value");
      valueNode.appendChild(doc.createTextNode(value));
      propNode.appendChild(valueNode);

      if (updatingResource != null) {
        String[] sources = updatingResource.get(name);
        if(sources != null) {
          for(String s : sources) {
            Element sourceNode = doc.createElement("source");
            sourceNode.appendChild(doc.createTextNode(s));
            propNode.appendChild(sourceNode);
          }
        }
      }
      
      conf.appendChild(doc.createTextNode("\n"));
    }
    return doc;
  }

  /**
   *  Writes out all the parameters and their properties (final and resource) to
   *  the given {@link Writer}
   *  The format of the output would be 
   *  { "properties" : [ {key1,value1,key1.isFinal,key1.resource}, {key2,value2,
   *  key2.isFinal,key2.resource}... ] } 
   *  It does not output the parameters of the configuration object which is 
   *  loaded from an input stream.
   * @param out the Writer to write to
   * @throws IOException
   */
  public static void dumpConfiguration(Configuration config,
      Writer out) throws IOException {
    JsonFactory dumpFactory = new JsonFactory();
    JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("properties");
    dumpGenerator.writeStartArray();
    dumpGenerator.flush();
    synchronized (config) {
      for (Map.Entry<Object,Object> item: config.getProps().entrySet()) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("key", (String) item.getKey());
        dumpGenerator.writeStringField("value", 
                                       config.get((String) item.getKey()));
        dumpGenerator.writeBooleanField("isFinal",
                                        config.finalParameters.contains(item.getKey()));
        String[] resources = config.updatingResource.get(item.getKey());
        String resource = UNKNOWN_RESOURCE;
        if(resources != null && resources.length > 0) {
          resource = resources[0];
        }
        dumpGenerator.writeStringField("resource", resource);
        dumpGenerator.writeEndObject();
      }
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
    dumpGenerator.flush();
  }
  
  /**
   * Get the {@link ClassLoader} for this job.
   * 
   * @return the correct class loader.
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }
  
  /**
   * Set the class loader that will be used to load the various objects.
   * 
   * @param classLoader the new class loader.
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Configuration: ");
    if(loadDefaults) {
      toString(defaultResources, sb);
      if(resources.size()>0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }
  
  private <T> void toString(List<T> resources, StringBuilder sb) {
    ListIterator<T> i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /** 
   * Set the quietness-mode. 
   * 
   * In the quiet-mode, error and informational messages might not be logged.
   * 
   * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
   *              to turn it off.
   */
  public synchronized void setQuietMode(boolean quietmode) {
    this.quietmode = quietmode;
  }

  synchronized boolean getQuietMode() {
    return this.quietmode;
  }
  
  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    new Configuration().writeXml(System.out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    int size = WritableUtils.readVInt(in);
    for(int i=0; i < size; ++i) {
      String key = org.apache.hadoop.io.Text.readString(in);
      String value = org.apache.hadoop.io.Text.readString(in);
      set(key, value); 
      String sources[] = WritableUtils.readCompressedStringArray(in);
      if(sources != null) {
        updatingResource.put(key, sources);
      }
    }
  }

  //@Override
  @Override
  public void write(DataOutput out) throws IOException {
    Properties props = getProps();
    WritableUtils.writeVInt(out, props.size());
    for(Map.Entry<Object, Object> item: props.entrySet()) {
      org.apache.hadoop.io.Text.writeString(out, (String) item.getKey());
      org.apache.hadoop.io.Text.writeString(out, (String) item.getValue());
      WritableUtils.writeCompressedStringArray(out, 
          updatingResource.get(item.getKey()));
    }
  }
  
  /**
   * get keys matching the the regex 
   * @param regex
   * @return Map<String,String> with matching keys
   */
  public Map<String,String> getValByRegex(String regex) {
    Pattern p = Pattern.compile(regex);

    Map<String,String> result = new HashMap<String,String>();
    Matcher m;

    for(Map.Entry<Object,Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof String && 
          item.getValue() instanceof String) {
        m = p.matcher((String)item.getKey());
        if(m.find()) { // match
          result.put((String) item.getKey(),
              substituteVars(getProps().getProperty((String) item.getKey())));
        }
      }
    }
    return result;
  }

  /**
   * A unique class which is used as a sentinel value in the caching
   * for getClassByName. {@see Configuration#getClassByNameOrNull(String)}
   */
  private static abstract class NegativeCacheSentinel {}

  public static void dumpDeprecatedKeys() {
    DeprecationContext deprecations = deprecationContext.get();
    for (Map.Entry<String, DeprecatedKeyInfo> entry :
        deprecations.getDeprecatedKeyMap().entrySet()) {
      StringBuilder newKeys = new StringBuilder();
      for (String newKey : entry.getValue().newKeys) {
        newKeys.append(newKey).append("\t");
      }
      System.out.println(entry.getKey() + "\t" + newKeys.toString());
    }
  }

  /**
   * Returns whether or not a deprecated name has been warned. If the name is not
   * deprecated then always return false
   */
  public static boolean hasWarnedDeprecation(String name) {
    DeprecationContext deprecations = deprecationContext.get();
    if(deprecations.getDeprecatedKeyMap().containsKey(name)) {
      if(deprecations.getDeprecatedKeyMap().get(name).accessed.get()) {
        return true;
      }
    }
    return false;
  }
}
