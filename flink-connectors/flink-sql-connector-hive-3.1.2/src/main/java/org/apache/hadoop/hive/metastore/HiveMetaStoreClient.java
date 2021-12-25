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

package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.hooks.URIResolverHook;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 *
 * This is copied from hive to port https://issues.apache.org/jira/browse/HIVE-21508
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveMetaStoreClient implements IMetaStoreClient, AutoCloseable {
  /**
   * Capabilities of the current client. If this client talks to a MetaStore server in a manner
   * implying the usage of some expanded features that require client-side support that this client
   * doesn't have (e.g. a getting a table of a new type), it will get back failures when the
   * capability checking is enabled (the default).
   */
  public final static ClientCapabilities VERSION = new ClientCapabilities(
      Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES));
  // Test capability for tests.
  public final static ClientCapabilities TEST_VERSION = new ClientCapabilities(
      Lists.newArrayList(ClientCapability.INSERT_ONLY_TABLES, ClientCapability.TEST_CAPABILITY));

  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean isConnected = false;
  private URI metastoreUris[];
  private final HiveMetaHookLoader hookLoader;
  protected final Configuration conf;  // Keep a copy of HiveConf so if Session conf changes, we may need to get a new HMS client.
  private String tokenStrForm;
  private final boolean localMetaStore;
  private final MetaStoreFilterHook filterHook;
  private final URIResolverHook uriResolverHook;
  private final int fileMetadataBatchSize;

  private Map<String, String> currentMetaVars;

  private static final AtomicInteger connCount = new AtomicInteger(0);

  // for thrift connects
  private int retries = 5;
  private long retryDelaySeconds = 0;
  private final ClientCapabilities version;

  static final protected Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClient.class);

  //copied from ErrorMsg.java
  private static final String REPL_EVENTS_MISSING_IN_METASTORE = "Notification events are missing in the meta store.";

  public HiveMetaStoreClient(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
    throws MetaException {

    this.hookLoader = hookLoader;
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
      this.conf = conf;
    } else {
      this.conf = new Configuration(conf);
    }
    version = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ? TEST_VERSION : VERSION;
    filterHook = loadFilterHooks();
    uriResolverHook = loadUriResolverHook();
    fileMetadataBatchSize = MetastoreConf.getIntVar(
        conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);

    String msUri = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    localMetaStore = MetastoreConf.isEmbeddedMetaStore(msUri);
    if (localMetaStore) {
      if (!allowEmbedded) {
        throw new MetaException("Embedded metastore is not allowed here. Please configure "
            + ConfVars.THRIFT_URIS.toString() + "; it is currently set to [" + msUri + "]");
      }
      // instantiate the metastore server handler directly instead of connecting
      // through the network
      client = HiveMetaStore.newRetryingHMSHandler("hive client", this.conf, true);
      isConnected = true;
      snapshotActiveConf();
      return;
    }

    // get the number retries
    retries = MetastoreConf.getIntVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES);
    retryDelaySeconds = MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

    // user wants file store based configuration
    if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS) != null) {
      resolveUris();
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }

    //If HADOOP_PROXY_USER is set in env or property,
    //then need to create metastore client that proxies as that user.
    String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";
    String proxyUser = System.getenv(HADOOP_PROXY_USER);
    if (proxyUser == null) {
      proxyUser = System.getProperty(HADOOP_PROXY_USER);
    }
    //if HADOOP_PROXY_USER is set, create DelegationToken using real user
    if(proxyUser != null) {
      LOG.info(HADOOP_PROXY_USER + " is set. Using delegation "
          + "token for HiveMetaStore connection.");
      try {
        UserGroupInformation.getLoginUser().getRealUser().doAs(
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                open();
                return null;
              }
            });
        String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
        String delegationTokenStr = getDelegationToken(proxyUser, proxyUser);
        SecurityUtils.setTokenStr(UserGroupInformation.getCurrentUser(), delegationTokenStr,
            delegationTokenPropString);
        MetastoreConf.setVar(this.conf, ConfVars.TOKEN_SIGNATURE, delegationTokenPropString);
        close();
      } catch (Exception e) {
        LOG.error("Error while setting delegation token for " + proxyUser, e);
        if(e instanceof MetaException) {
          throw (MetaException)e;
        } else {
          throw new MetaException(e.getMessage());
        }
      }
    }
    // finally open the store
    open();
  }

  private void resolveUris() throws MetaException {
    String metastoreUrisString[] =  MetastoreConf.getVar(conf,
            ConfVars.THRIFT_URIS).split(",");

    List<URI> metastoreURIArray = new ArrayList<URI>();
    try {
      int i = 0;
      for (String s : metastoreUrisString) {
        URI tmpUri = new URI(s);
        if (tmpUri.getScheme() == null) {
          throw new IllegalArgumentException("URI: " + s
                  + " does not have a scheme");
        }
        if (uriResolverHook != null) {
          metastoreURIArray.addAll(uriResolverHook.resolveURI(tmpUri));
        } else {
          metastoreURIArray.add(new URI(
                  tmpUri.getScheme(),
                  tmpUri.getUserInfo(),
                  HadoopThriftAuthBridge.getBridge().getCanonicalHostName(tmpUri.getHost()),
                  tmpUri.getPort(),
                  tmpUri.getPath(),
                  tmpUri.getQuery(),
                  tmpUri.getFragment()
          ));
        }
      }
      metastoreUris = new URI[metastoreURIArray.size()];
      for (int j = 0; j < metastoreURIArray.size(); j++) {
        metastoreUris[j] = metastoreURIArray.get(j);
      }

      if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
        List<URI> uriList = Arrays.asList(metastoreUris);
        Collections.shuffle(uriList);
        metastoreUris = uriList.toArray(new URI[uriList.size()]);
      }
    } catch (IllegalArgumentException e) {
      throw (e);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }


  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
    Class<? extends MetaStoreFilterHook> authProviderClass = MetastoreConf.
        getClass(conf, ConfVars.FILTER_HOOK, DefaultMetaStoreFilterHookImpl.class,
            MetaStoreFilterHook.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends MetaStoreFilterHook> constructor =
          authProviderClass.getConstructor(Configuration.class);
      return constructor.newInstance(conf);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    }
  }

  //multiple clients may initialize the hook at the same time
  synchronized private URIResolverHook loadUriResolverHook() throws IllegalStateException {

    String uriResolverClassName =
            MetastoreConf.getAsString(conf, ConfVars.URI_RESOLVER);
    if (uriResolverClassName.equals("")) {
      return null;
    } else {
      LOG.info("Loading uri resolver" + uriResolverClassName);
      try {
        Class<?> uriResolverClass = Class.forName(uriResolverClassName, true,
                JavaUtils.getClassLoader());
        return (URIResolverHook) ReflectionUtils.newInstance(uriResolverClass, null);
      } catch (Exception e) {
        LOG.error("Exception loading uri resolver hook" + e);
        return null;
      }
    }
  }

  /**
   * Swaps the first element of the metastoreUris array with a random element from the
   * remainder of the array.
   */
  private void promoteRandomMetaStoreURI() {
    if (metastoreUris.length <= 1) {
      return;
    }
    Random rng = new Random();
    int index = rng.nextInt(metastoreUris.length - 1) + 1;
    URI tmp = metastoreUris[0];
    metastoreUris[0] = metastoreUris[index];
    metastoreUris[index] = tmp;
  }

  @VisibleForTesting
  public TTransport getTTransport() {
    return transport;
  }

  @Override
  public boolean isLocalMetaStore() {
    return localMetaStore;
  }

  @Override
  public boolean isCompatibleWith(Configuration conf) {
    // Make a copy of currentMetaVars, there is a race condition that
	// currentMetaVars might be changed during the execution of the method
    Map<String, String> currentMetaVarsCopy = currentMetaVars;
    if (currentMetaVarsCopy == null) {
      return false; // recreate
    }
    boolean compatible = true;
    for (ConfVars oneVar : MetastoreConf.metaVars) {
      // Since metaVars are all of different types, use string for comparison
      String oldVar = currentMetaVarsCopy.get(oneVar.getVarname());
      String newVar = MetastoreConf.getAsString(conf, oneVar);
      if (oldVar == null ||
          (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
        LOG.info("Mestastore configuration " + oneVar.toString() +
            " changed from " + oldVar + " to " + newVar);
        compatible = false;
      }
    }
    return compatible;
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    MetastoreConf.setVar(conf, ConfVars.ADDED_JARS, addedJars);
  }

  @Override
  public void reconnect() throws MetaException {
    if (localMetaStore) {
      // For direct DB connections we don't yet support reestablishing connections.
      throw new MetaException("For direct MetaStore DB connections, we don't support retries" +
          " at the client level.");
    } else {
      close();

      if (uriResolverHook != null) {
        //for dynamic uris, re-lookup if there are new metastore locations
        resolveUris();
      }

      if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
        // Swap the first element of the metastoreUris[] with a random element from the rest
        // of the array. Rationale being that this method will generally be called when the default
        // connection has died and the default connection is likely to be the first array element.
        promoteRandomMetaStoreURI();
      }
      open();
    }
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl) throws TException {
    alter_table_with_environmentContext(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table,
                          boolean cascade) throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    if (cascade) {
      environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
  }

  @Override
  public void alter_table_with_environmentContext(String dbname, String tbl_name, Table new_tbl,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    HiveMetaHook hook = getHook(new_tbl);
    if (hook != null) {
      hook.preAlterTable(new_tbl, envContext);
    }
    client.alter_table_with_environment_context(prependCatalogToDbName(dbname, conf),
        tbl_name, new_tbl, envContext);
  }

  @Override
  public void alter_table(String catName, String dbName, String tblName, Table newTable,
                         EnvironmentContext envContext) throws TException {
    client.alter_table_with_environment_context(prependCatalogToDbName(catName,
        dbName, conf), tblName, newTable, envContext);
  }

  @Override
  public void renamePartition(final String dbname, final String tableName, final List<String> part_vals,
                              final Partition newPart) throws TException {
    renamePartition(getDefaultCatalog(conf), dbname, tableName, part_vals, newPart);
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
                              Partition newPart) throws TException {
    client.rename_partition(prependCatalogToDbName(catName, dbname, conf), tableName, part_vals, newPart);

  }

  private void open() throws MetaException {
    isConnected = false;
    TTransportException tte = null;
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
    boolean useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
      for (URI store : metastoreUris) {
        LOG.info("Trying to connect to metastore with URI " + store);

        try {
          if (useSSL) {
            try {
              String trustStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
              if (trustStorePath.isEmpty()) {
                throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH.toString()
                    + " Not configured for SSL connection");
              }
              String trustStorePassword =
                  MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);

              // Create an SSL socket and connect
              transport = SecurityUtils.getSSLSocket(store.getHost(), store.getPort(), clientSocketTimeout,
                  trustStorePath, trustStorePassword );
              LOG.info("Opened an SSL connection to metastore, current connections: " + connCount.incrementAndGet());
            } catch(IOException e) {
              throw new IllegalArgumentException(e);
            } catch(TTransportException e) {
              tte = e;
              throw new MetaException(e.toString());
            }
          } else {
            transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
          }

          if (useSasl) {
            // Wrap thrift connection with SASL for secure connection.
            try {
              HadoopThriftAuthBridge.Client authBridge =
                HadoopThriftAuthBridge.getBridge().createClient();

              // check if we should use delegation tokens to authenticate
              // the call below gets hold of the tokens if they are set up by hadoop
              // this should happen on the map/reduce tasks if the client added the
              // tokens into hadoop's credential store in the front end during job
              // submission.
              String tokenSig = MetastoreConf.getVar(conf, ConfVars.TOKEN_SIGNATURE);
              // tokenSig could be null
              tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);

              if(tokenStrForm != null) {
                LOG.info("HMSC::open(): Found delegation token. Creating DIGEST-based thrift connection.");
                // authenticate using delegation tokens via the "DIGEST" mechanism
                transport = authBridge.createClientTransport(null, store.getHost(),
                    "DIGEST", tokenStrForm, transport,
                        MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
              } else {
                LOG.info("HMSC::open(): Could not find delegation token. Creating KERBEROS-based thrift connection.");
                String principalConfig =
                    MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);
                transport = authBridge.createClientTransport(
                    principalConfig, store.getHost(), "KERBEROS", null,
                    transport, MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
              }
            } catch (IOException ioe) {
              LOG.error("Couldn't create client transport", ioe);
              throw new MetaException(ioe.toString());
            }
          } else {
            if (useFramedTransport) {
              transport = new TFramedTransport(transport);
            }
          }

          final TProtocol protocol;
          if (useCompactProtocol) {
            protocol = new TCompactProtocol(transport);
          } else {
            protocol = new TBinaryProtocol(transport);
          }
          client = new ThriftHiveMetastore.Client(protocol);
          try {
            if (!transport.isOpen()) {
              transport.open();
              LOG.info("Opened a connection to metastore, current connections: " + connCount.incrementAndGet());
            }
            isConnected = true;
          } catch (TTransportException e) {
            tte = e;
            if (LOG.isDebugEnabled()) {
              LOG.warn("Failed to connect to the MetaStore Server...", e);
            } else {
              // Don't print full exception trace if DEBUG is not on.
              LOG.warn("Failed to connect to the MetaStore Server...");
            }
          }

          if (isConnected && !useSasl && MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)){
            // Call set_ugi, only in unsecure mode.
            try {
              UserGroupInformation ugi = SecurityUtils.getUGI();
              client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
            } catch (LoginException e) {
              LOG.warn("Failed to do login. set_ugi() is not successful, " +
                       "Continuing without it.", e);
            } catch (IOException e) {
              LOG.warn("Failed to find ugi of client set_ugi() is not successful, " +
                  "Continuing without it.", e);
            } catch (TException e) {
              LOG.warn("set_ugi() not successful, Likely cause: new client talking to old server. "
                  + "Continuing without it.", e);
            }
          }
        } catch (MetaException e) {
          LOG.error("Unable to connect to metastore with URI " + store
                    + " in attempt " + attempt, e);
        }
        if (isConnected) {
          break;
        }
      }
      // Wait before launching the next round of connection retries.
      if (!isConnected && retryDelaySeconds > 0) {
        try {
          LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
          Thread.sleep(retryDelaySeconds * 1000);
        } catch (InterruptedException ignore) {}
      }
    }

    if (!isConnected) {
      throw new MetaException("Could not connect to meta store using any of the URIs provided." +
        " Most recent failure: " + StringUtils.stringifyException(tte));
    }

    snapshotActiveConf();

    LOG.info("Connected to metastore.");
  }

  private void snapshotActiveConf() {
    currentMetaVars = new HashMap<>(MetastoreConf.metaVars.length);
    for (ConfVars oneVar : MetastoreConf.metaVars) {
      currentMetaVars.put(oneVar.getVarname(), MetastoreConf.getAsString(conf, oneVar));
    }
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return tokenStrForm;
   }

  @Override
  public void close() {
    isConnected = false;
    currentMetaVars = null;
    try {
      if (null != client) {
        client.shutdown();
      }
    } catch (TException e) {
      LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
    }
    // Transport would have got closed via client.shutdown(), so we dont need this, but
    // just in case, we make this call.
    if ((transport != null) && transport.isOpen()) {
      transport.close();
      LOG.info("Closed a connection to metastore, current connections: " + connCount.decrementAndGet());
    }
  }

  @Override
  public void setMetaConf(String key, String value) throws TException {
    client.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws TException {
    return client.getMetaConf(key);
  }

  @Override
  public void createCatalog(Catalog catalog) throws TException {
    client.create_catalog(new CreateCatalogRequest(catalog));
  }

  @Override
  public void alterCatalog(String catalogName, Catalog newCatalog) throws TException {
    client.alter_catalog(new AlterCatalogRequest(catalogName, newCatalog));
  }

  @Override
  public Catalog getCatalog(String catName) throws TException {
    GetCatalogResponse rsp = client.get_catalog(new GetCatalogRequest(catName));
    return rsp == null ? null : filterHook.filterCatalog(rsp.getCatalog());
  }

  @Override
  public List<String> getCatalogs() throws TException {
    GetCatalogsResponse rsp = client.get_catalogs();
    return rsp == null ? null : filterHook.filterCatalogs(rsp.getNames());
  }

  @Override
  public void dropCatalog(String catName) throws TException {
    client.drop_catalog(new DropCatalogRequest(catName));
  }

  /**
   * @param new_part
   * @return the added partition
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition(org.apache.hadoop.hive.metastore.api.Partition)
   */
  @Override
  public Partition add_partition(Partition new_part) throws TException {
    return add_partition(new_part, null);
  }

  public Partition add_partition(Partition new_part, EnvironmentContext envContext)
      throws TException {
    if (!new_part.isSetCatName()) new_part.setCatName(getDefaultCatalog(conf));
    Partition p = client.add_partition_with_environment_context(new_part, envContext);
    return deepCopy(p);
  }

  /**
   * @param new_parts
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partitions(List)
   */
  @Override
  public int add_partitions(List<Partition> new_parts) throws TException {
    if (new_parts != null && !new_parts.isEmpty() && !new_parts.get(0).isSetCatName()) {
      final String defaultCat = getDefaultCatalog(conf);
      new_parts.forEach(p -> p.setCatName(defaultCat));
    }
    return client.add_partitions(new_parts);
  }

  @Override
  public List<Partition> add_partitions(
      List<Partition> parts, boolean ifNotExists, boolean needResults) throws TException {
    if (parts.isEmpty()) {
      return needResults ? new ArrayList<>() : null;
    }
    Partition part = parts.get(0);
    // Have to set it for each partition too
    if (!part.isSetCatName()) {
      final String defaultCat = getDefaultCatalog(conf);
      parts.forEach(p -> p.setCatName(defaultCat));
    }
    AddPartitionsRequest req = new AddPartitionsRequest(
        part.getDbName(), part.getTableName(), parts, ifNotExists);
    req.setCatName(part.isSetCatName() ? part.getCatName() : getDefaultCatalog(conf));
    req.setNeedResult(needResults);
    AddPartitionsResult result = client.add_partitions_req(req);
    return needResults ? filterHook.filterPartitions(result.getPartitions()) : null;
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    if (partitionSpec.getCatName() == null) {
      partitionSpec.setCatName(getDefaultCatalog(conf));
    }
    return client.add_partitions_pspec(partitionSpec.toPartitionSpec());
  }

  @Override
  public Partition appendPartition(String db_name, String table_name,
      List<String> part_vals) throws TException {
    return appendPartition(getDefaultCatalog(conf), db_name, table_name, part_vals);
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, String partName)
      throws TException {
    return appendPartition(getDefaultCatalog(conf), dbName, tableName, partName);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName,
                                   String name) throws TException {
    Partition p = client.append_partition_by_name(prependCatalogToDbName(
        catName, dbName, conf), tableName, name);
    return deepCopy(p);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName,
                                   List<String> partVals) throws TException {
    Partition p = client.append_partition(prependCatalogToDbName(
        catName, dbName, conf), tableName, partVals);
    return deepCopy(p);
  }

  @Deprecated
  public Partition appendPartition(String dbName, String tableName, List<String> partVals,
                                   EnvironmentContext ec) throws TException {
    return client.append_partition_with_environment_context(prependCatalogToDbName(dbName, conf),
        tableName, partVals, ec).deepCopy();
  }

  /**
   * Exchange the partition between two tables
   * @param partitionSpecs partitions specs of the parent partition to be exchanged
   * @param destDb the db of the destination table
   * @param destinationTableName the destination table name
   * @return new partition after exchanging
   */
  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName) throws TException {
    return exchange_partition(partitionSpecs, getDefaultCatalog(conf), sourceDb, sourceTable,
        getDefaultCatalog(conf), destDb, destinationTableName);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat,
                                      String sourceDb, String sourceTable, String destCat,
                                      String destDb, String destTableName) throws TException {
    return client.exchange_partition(partitionSpecs, prependCatalogToDbName(sourceCat, sourceDb, conf),
        sourceTable, prependCatalogToDbName(destCat, destDb, conf), destTableName);
  }

  /**
   * Exchange the partitions between two tables
   * @param partitionSpecs partitions specs of the parent partition to be exchanged
   * @param destDb the db of the destination table
   * @param destinationTableName the destination table name
   * @return new partitions after exchanging
   */
  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName) throws TException {
    return exchange_partitions(partitionSpecs, getDefaultCatalog(conf), sourceDb, sourceTable,
        getDefaultCatalog(conf), destDb, destinationTableName);
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat,
                                             String sourceDb, String sourceTable, String destCat,
                                             String destDb, String destTableName) throws TException {
    return client.exchange_partitions(partitionSpecs, prependCatalogToDbName(sourceCat, sourceDb, conf),
        sourceTable, prependCatalogToDbName(destCat, destDb, conf), destTableName);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals)
      throws TException, MetaException {
    client.partition_name_has_valid_characters(partVals, true);
  }

  /**
   * Create a new Database
   * @param db
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(Database)
   */
  @Override
  public void createDatabase(Database db)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    if (!db.isSetCatalogName()) {
      db.setCatalogName(getDefaultCatalog(conf));
    }
    client.create_database(db);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  @Override
  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(tbl, null);
  }

  public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(conf));
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(tbl);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      create_table_with_environment_context(tbl, envContext);
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    }
    finally {
      if (!success && (hook != null)) {
        try {
          hook.rollbackCreateTable(tbl);
        } catch (Exception e){
          LOG.error("Create rollback failed with", e);
        }
      }
    }
  }

  @Override
  public void createTableWithConstraints(Table tbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints,
    List<SQLCheckConstraint> checkConstraints)
        throws AlreadyExistsException, InvalidObjectException,
        MetaException, NoSuchObjectException, TException {

    if (!tbl.isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      tbl.setCatName(defaultCat);
      if (primaryKeys != null) {
        primaryKeys.forEach(pk -> pk.setCatName(defaultCat));
      }
      if (foreignKeys != null) {
        foreignKeys.forEach(fk -> fk.setCatName(defaultCat));
      }
      if (uniqueConstraints != null) {
        uniqueConstraints.forEach(uc -> uc.setCatName(defaultCat));
      }
      if (notNullConstraints != null) {
        notNullConstraints.forEach(nn -> nn.setCatName(defaultCat));
      }
      if (defaultConstraints != null) {
        defaultConstraints.forEach(def -> def.setCatName(defaultCat));
      }
      if (checkConstraints != null) {
        checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
      }
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(tbl);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      client.create_table_with_constraints(tbl, primaryKeys, foreignKeys,
          uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackCreateTable(tbl);
      }
    }
  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws TException {
    dropConstraint(getDefaultCatalog(conf), dbName, tableName, constraintName);
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws TException {
    DropConstraintRequest rqst = new DropConstraintRequest(dbName, tableName, constraintName);
    rqst.setCatName(catName);
    client.drop_constraint(rqst);
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
    if (!primaryKeyCols.isEmpty() && !primaryKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      primaryKeyCols.forEach(pk -> pk.setCatName(defaultCat));
    }
    client.add_primary_key(new AddPrimaryKeyRequest(primaryKeyCols));
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
    if (!foreignKeyCols.isEmpty() && !foreignKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      foreignKeyCols.forEach(fk -> fk.setCatName(defaultCat));
    }
    client.add_foreign_key(new AddForeignKeyRequest(foreignKeyCols));
  }

  @Override
  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws
    NoSuchObjectException, MetaException, TException {
    if (!uniqueConstraintCols.isEmpty() && !uniqueConstraintCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      uniqueConstraintCols.forEach(uc -> uc.setCatName(defaultCat));
    }
    client.add_unique_constraint(new AddUniqueConstraintRequest(uniqueConstraintCols));
  }

  @Override
  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws
    NoSuchObjectException, MetaException, TException {
    if (!notNullConstraintCols.isEmpty() && !notNullConstraintCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      notNullConstraintCols.forEach(nn -> nn.setCatName(defaultCat));
    }
    client.add_not_null_constraint(new AddNotNullConstraintRequest(notNullConstraintCols));
  }

  @Override
  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws
      NoSuchObjectException, MetaException, TException {
    if (!defaultConstraints.isEmpty() && !defaultConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      defaultConstraints.forEach(def -> def.setCatName(defaultCat));
    }
    client.add_default_constraint(new AddDefaultConstraintRequest(defaultConstraints));
  }

  @Override
  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws
      NoSuchObjectException, MetaException, TException {
    if (!checkConstraints.isEmpty() && !checkConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
    }
    client.add_check_constraint(new AddCheckConstraintRequest(checkConstraints));
  }

  /**
   * @param type
   * @return true or false
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(org.apache.hadoop.hive.metastore.api.Type)
   */
  public boolean createType(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return client.create_type(type);
  }

  /**
   * @param name
   * @throws NoSuchObjectException
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String, boolean, boolean)
   */
  @Override
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(getDefaultCatalog(conf), name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean deleteData,
                           boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getDatabase(catalogName, dbName);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownDb) {
        throw e;
      }
      return;
    }

    if (cascade) {
      // Note that this logic may drop some of the tables of the database
      // even if the drop database fail for any reason
      // TODO: Fix this
      List<String> materializedViews = getTables(dbName, ".*", TableType.MATERIALIZED_VIEW);
      for (String table : materializedViews) {
        // First we delete the materialized views
        dropTable(dbName, table, deleteData, true);
      }
      List<String> tableList = getAllTables(dbName);
      for (String table : tableList) {
        // Now we delete the rest of tables
        try {
          // Subclasses can override this step (for example, for temporary tables)
          dropTable(dbName, table, deleteData, true);
        } catch (UnsupportedOperationException e) {
          // Ignore Index tables, those will be dropped with parent tables
        }
      }
    }
    client.drop_database(prependCatalogToDbName(catalogName, dbName, conf), deleteData, cascade);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData)
      throws TException {
    return dropPartition(getDefaultCatalog(conf), dbName, tableName, partName, deleteData);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, String name,
                               boolean deleteData) throws TException {
    return client.drop_partition_by_name_with_environment_context(prependCatalogToDbName(
        catName, db_name, conf), tbl_name, name, deleteData, null);
  }

  private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
    Map<String, String> warehouseOptions = new HashMap<>();
    warehouseOptions.put("ifPurge", "TRUE");
    return new EnvironmentContext(warehouseOptions);
  }

  // A bunch of these are in HiveMetaStoreClient but not IMetaStoreClient.  I have marked these
  // as deprecated and not updated them for the catalogs.  If we really want to support them we
  // should add them to IMetaStoreClient.

  @Deprecated
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      EnvironmentContext env_context) throws TException {
    return client.drop_partition_with_environment_context(prependCatalogToDbName(db_name, conf),
        tbl_name, part_vals, true, env_context);
  }

  @Deprecated
  public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData,
                               EnvironmentContext ec) throws TException {
    return client.drop_partition_by_name_with_environment_context(prependCatalogToDbName(dbName, conf),
        tableName, partName, dropData, ec);
  }

  @Deprecated
  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws TException {
    return client.drop_partition(prependCatalogToDbName(dbName, conf), tableName, partVals, true);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        PartitionDropOptions.instance().deleteData(deleteData));
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name,
                               List<String> part_vals, boolean deleteData) throws TException {
    return dropPartition(catName, db_name, tbl_name, part_vals, PartitionDropOptions.instance()
            .deleteData(deleteData));
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name,
                               List<String> part_vals, PartitionDropOptions options) throws TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals, options);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name,
                               List<String> part_vals, PartitionDropOptions options)
      throws TException {
    if (options == null) {
      options = PartitionDropOptions.instance();
    }
    if (part_vals != null) {
      for (String partVal : part_vals) {
        if (partVal == null) {
          throw new MetaException("The partition value must not be null.");
        }
      }
    }
    return client.drop_partition_with_environment_context(prependCatalogToDbName(
        catName, db_name, conf), tbl_name, part_vals, options.deleteData,
        options.purgeData ? getEnvironmentContextWithIfPurgeSet() : null);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
                                        List<ObjectPair<Integer, byte[]>> partExprs,
                                        PartitionDropOptions options)
      throws TException {
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs, options);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {

    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ifExists(ifExists)
                                              .returnResults(needResult));

  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists) throws NoSuchObjectException, MetaException, TException {
    // By default, we need the results from dropPartitions();
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ifExists(ifExists));
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                        List<ObjectPair<Integer, byte[]>> partExprs,
                                        PartitionDropOptions options) throws TException {
    RequestPartsSpec rps = new RequestPartsSpec();
    List<DropPartitionsExpr> exprs = new ArrayList<>(partExprs.size());
    for (ObjectPair<Integer, byte[]> partExpr : partExprs) {
      DropPartitionsExpr dpe = new DropPartitionsExpr();
      dpe.setExpr(partExpr.getSecond());
      dpe.setPartArchiveLevel(partExpr.getFirst());
      exprs.add(dpe);
    }
    rps.setExprs(exprs);
    DropPartitionsRequest req = new DropPartitionsRequest(dbName, tblName, rps);
    req.setCatName(catName);
    req.setDeleteData(options.deleteData);
    req.setNeedResult(options.returnResults);
    req.setIfExists(options.ifExists);
    if (options.purgeData) {
      LOG.info("Dropped partitions will be purged!");
      req.setEnvironmentContext(getEnvironmentContextWithIfPurgeSet());
    }
    return client.drop_partitions_req(req).getPartitions();
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, null);
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(String dbname, String name) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, true, true, null);
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                        boolean ignoreUnknownTable, boolean ifPurge) throws TException {
    //build new environmentContext with ifPurge;
    EnvironmentContext envContext = null;
    if(ifPurge){
      Map<String, String> warehouseOptions;
      warehouseOptions = new HashMap<>();
      warehouseOptions.put("ifPurge", "TRUE");
      envContext = new EnvironmentContext(warehouseOptions);
    }
    dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, envContext);

  }

  /**
   * Drop the table and choose whether to: delete the underlying table data;
   * throw if the table doesn't exist; save the data in the trash.
   *
   * @param catName catalog name
   * @param dbname database name
   * @param name table name
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @param ignoreUnknownTab
   *          don't throw if the requested table doesn't exist
   * @param envContext
   *          for communicating with thrift
   * @throws MetaException
   *           could not drop table properly
   * @throws NoSuchObjectException
   *           the table wasn't found
   * @throws TException
   *           a thrift communication error occurred
   * @throws UnsupportedOperationException
   *           dropping an index table is not allowed
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void dropTable(String catName, String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    Table tbl;
    try {
      tbl = getTable(catName, dbname, name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
      return;
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preDropTable(tbl);
    }
    boolean success = false;
    try {
      drop_table_with_environment_context(catName, dbname, name, deleteData, envContext);
      if (hook != null) {
        hook.commitDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
      }
      success=true;
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackDropTable(tbl);
      }
    }
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, partNames);
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws TException {
    client.truncate_table(prependCatalogToDbName(catName, dbName, conf), tableName, partNames);
  }

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param request Inputs for path of the data files to be recycled to cmroot and
   *                isPurge flag when set to true files which needs to be recycled are not moved to Trash
   * @return Response which is currently void
   */
  @Override
  public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
    return client.cm_recycle(request);
  }

  /**
   * @param type
   * @return true if the type is dropped
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
   */
  public boolean dropType(String type) throws NoSuchObjectException, MetaException, TException {
    return client.drop_type(type);
  }

  /**
   * @param name
   * @return map of types
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
   */
  public Map<String, Type> getTypeAll(String name) throws MetaException,
      TException {
    Map<String, Type> result = null;
    Map<String, Type> fromClient = client.get_type_all(name);
    if (fromClient != null) {
      result = new LinkedHashMap<>();
      for (String key : fromClient.keySet()) {
        result.put(key, deepCopy(fromClient.get(key)));
      }
    }
    return result;
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws TException {
    return getDatabases(getDefaultCatalog(conf), databasePattern);
  }

  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws TException {
    return filterHook.filterDatabases(client.get_databases(prependCatalogToDbName(
        catName, databasePattern, conf)));
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return getAllDatabases(getDefaultCatalog(conf));
  }

  @Override
  public List<String> getAllDatabases(String catName) throws TException {
    return filterHook.filterDatabases(client.get_databases(prependCatalogToDbName(catName, null, conf)));
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                        int max_parts) throws TException {
    List<Partition> parts = client.get_partitions(prependCatalogToDbName(catName, db_name, conf),
        tbl_name, shrinkMaxtoShort(max_parts));
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    return listPartitionSpecs(getDefaultCatalog(conf), dbName, tableName, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
                                               int maxParts) throws TException {
    return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
        client.get_partitions_pspec(prependCatalogToDbName(catName, dbName, conf), tableName, maxParts)));
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
                                        List<String> part_vals, short max_parts) throws TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                        List<String> part_vals, int max_parts) throws TException {
    List<Partition> parts = client.get_partitions_ps(prependCatalogToDbName(catName, db_name, conf),
        tbl_name, part_vals, shrinkMaxtoShort(max_parts));
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name,
                                                    short max_parts, String user_name,
                                                    List<String> group_names) throws TException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, max_parts, user_name,
        group_names);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                    int maxParts, String userName,
                                                    List<String> groupNames) throws TException {
    List<Partition> parts = client.get_partitions_with_auth(prependCatalogToDbName(catName,
        dbName, conf), tableName, shrinkMaxtoShort(maxParts), userName, groupNames);
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name,
                                                    List<String> part_vals, short max_parts,
                                                    String user_name, List<String> group_names)
      throws TException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts,
        user_name, group_names);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                                    List<String> partialPvals, int maxParts,
                                                    String userName, List<String> groupNames)
      throws TException {
    List<Partition> parts = client.get_partitions_ps_with_auth(prependCatalogToDbName(catName,
        dbName, conf), tableName, partialPvals, shrinkMaxtoShort(maxParts), userName, groupNames);
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws TException {
    return listPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                                String filter, int max_parts) throws TException {
    List<Partition> parts =client.get_partitions_by_filter(prependCatalogToDbName(
        catName, db_name, conf), tbl_name, filter, shrinkMaxtoShort(max_parts));
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                       String filter, int max_parts)
      throws TException {
    return listPartitionSpecsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name,
                                                       String tbl_name, String filter,
                                                       int max_parts) throws TException {
    return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
        client.get_part_specs_by_filter(prependCatalogToDbName(catName, db_name, conf), tbl_name, filter,
            max_parts)));
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
                                      String default_partition_name, short max_parts,
                                      List<Partition> result) throws TException {
    return listPartitionsByExpr(getDefaultCatalog(conf), db_name, tbl_name, expr,
        default_partition_name, max_parts, result);
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result)
          throws TException {
    assert result != null;
    PartitionsByExprRequest req = new PartitionsByExprRequest(
        db_name, tbl_name, ByteBuffer.wrap(expr));
    if (default_partition_name != null) {
      req.setDefaultPartitionName(default_partition_name);
    }
    if (max_parts >= 0) {
      req.setMaxParts(shrinkMaxtoShort(max_parts));
    }
    PartitionsByExprResult r;
    try {
      r = client.get_partitions_by_expr(req);
    } catch (TApplicationException te) {
      // TODO: backward compat for Hive <= 0.12. Can be removed later.
      if (te.getType() != TApplicationException.UNKNOWN_METHOD
          && te.getType() != TApplicationException.WRONG_METHOD_NAME) {
        throw te;
      }
      throw new IncompatibleMetastoreException(
          "Metastore doesn't support listPartitionsByExpr: " + te.getMessage());
    }
    r.setPartitions(filterHook.filterPartitions(r.getPartitions()));
    // TODO: in these methods, do we really need to deepcopy?
    deepCopyPartitions(r.getPartitions(), result);
    return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions(); // Assume the worst.
  }

  @Override
  public Database getDatabase(String name) throws TException {
    return getDatabase(getDefaultCatalog(conf), name);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) throws TException {
    Database d = client.get_database(prependCatalogToDbName(catalogName, databaseName, conf));
    return deepCopy(filterHook.filterDatabase(d));
  }

  @Override
  public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
      throws TException {
    return getPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName,
                                List<String> partVals) throws TException {
    Partition p = client.get_partition(prependCatalogToDbName(catName, dbName, conf), tblName, partVals);
    return deepCopy(filterHook.filterPartition(p));
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws TException {
    return getPartitionsByNames(getDefaultCatalog(conf), db_name, tbl_name, part_names);
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
                                              List<String> part_names) throws TException {
    List<Partition> parts =
        client.get_partitions_by_names(prependCatalogToDbName(catName, db_name, conf), tbl_name, part_names);
    return deepCopyPartitions(filterHook.filterPartitions(parts));
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException {
    if (!request.isSetCatName()) {
      request.setCatName(getDefaultCatalog(conf));
    }
    return client.get_partition_values(request);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String db_name, String tbl_name,
      List<String> part_vals, String user_name, List<String> group_names)
      throws TException {
    return getPartitionWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        user_name, group_names);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                            List<String> pvals, String userName,
                                            List<String> groupNames) throws TException {
    Partition p = client.get_partition_with_auth(prependCatalogToDbName(catName, dbName, conf), tableName,
        pvals, userName, groupNames);
    return deepCopy(filterHook.filterPartition(p));
  }

  @Override
  public Table getTable(String dbname, String name) throws TException {
    return getTable(getDefaultCatalog(conf), dbname, name);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setCapabilities(version);
    Table t = client.get_table_req(req).getTable();
    return deepCopy(filterHook.filterTable(t));
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws TException {
    return getTableObjectsByName(getDefaultCatalog(conf), dbName, tableNames);
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName,
                                           List<String> tableNames) throws TException {
    GetTablesRequest req = new GetTablesRequest(dbName);
    req.setCatName(catName);
    req.setTblNames(tableNames);
    req.setCapabilities(version);
    List<Table> tabs = client.get_table_objects_by_name_req(req).getTables();
    return deepCopyTables(filterHook.filterTables(tabs));
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return client.get_materialization_invalidation_info(cm, validTxnList);
  }

  @Override
  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    client.update_creation_metadata(getDefaultCatalog(conf), dbName, tableName, cm);
  }

  @Override
  public void updateCreationMetadata(String catName, String dbName, String tableName,
                                     CreationMetadata cm) throws MetaException, TException {
    client.update_creation_metadata(catName, dbName, tableName, cm);

  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException {
    return listTableNamesByFilter(getDefaultCatalog(conf), dbName, filter, maxTables);
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                             int maxTables) throws TException {
    return filterHook.filterTableNames(catName, dbName,
        client.get_table_names_by_filter(prependCatalogToDbName(catName, dbName, conf), filter,
            shrinkMaxtoShort(maxTables)));
  }

  /**
   * @param name
   * @return the type
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type(java.lang.String)
   */
  public Type getType(String name) throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_type(name));
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern)
      throws TException {
    return filterHook.filterTableNames(catName, dbName,
        client.get_tables(prependCatalogToDbName(catName, dbName, conf), tablePattern));
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern, tableType);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern,
                                TableType tableType) throws TException {
    return filterHook.filterTableNames(catName, dbName,
        client.get_tables_by_type(prependCatalogToDbName(catName, dbName, conf), tablePattern,
            tableType.toString()));
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName) throws TException {
    return getMaterializedViewsForRewriting(getDefaultCatalog(conf), dbName);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbname)
      throws MetaException {
    try {
      return filterHook.filterTableNames(catName, dbname,
          client.get_materialized_views_for_rewriting(prependCatalogToDbName(catName, dbname, conf)));
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException {
    try {
      return getTableMeta(getDefaultCatalog(conf), dbPatterns, tablePatterns, tableTypes);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                      List<String> tableTypes) throws TException {
    return filterHook.filterTableMetas(client.get_table_meta(prependCatalogToDbName(
        catName, dbPatterns, conf), tablePatterns, tableTypes));
  }

  @Override
  public List<String> getAllTables(String dbname) throws MetaException {
    try {
      return getAllTables(getDefaultCatalog(conf), dbname);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws TException {
    return filterHook.filterTableNames(catName, dbName, client.get_all_tables(
        prependCatalogToDbName(catName, dbName, conf)));
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return tableExists(getDefaultCatalog(conf), databaseName, tableName);
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) throws TException {
    try {
      GetTableRequest req = new GetTableRequest(dbName, tableName);
      req.setCatName(catName);
      req.setCapabilities(version);
      return filterHook.filterTable(client.get_table_req(req).getTable()) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName,
      short max) throws NoSuchObjectException, MetaException, TException {
    return listPartitionNames(getDefaultCatalog(conf), dbName, tblName, max);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName,
                                         int maxParts) throws TException {
    return filterHook.filterPartitionNames(catName, dbName, tableName,
        client.get_partition_names(prependCatalogToDbName(catName, dbName, conf), tableName, shrinkMaxtoShort(maxParts)));
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                         List<String> part_vals, int max_parts) throws TException {
    return filterHook.filterPartitionNames(catName, db_name, tbl_name,
        client.get_partition_names_ps(prependCatalogToDbName(catName, db_name, conf), tbl_name,
            part_vals, shrinkMaxtoShort(max_parts)));
  }

  @Override
  public int getNumPartitionsByFilter(String db_name, String tbl_name,
                                      String filter) throws TException {
    return getNumPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter);
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName,
                                      String filter) throws TException {
    return client.get_num_partitions_by_filter(prependCatalogToDbName(catName, dbName, conf), tableName,
        filter);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, null);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, environmentContext);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
                              EnvironmentContext environmentContext) throws TException {
    client.alter_partition_with_environment_context(prependCatalogToDbName(catName, dbName, conf), tblName,
        newPart, environmentContext);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws TException {
    alter_partitions(getDefaultCatalog(conf), dbName, tblName, newParts, null);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
                               EnvironmentContext environmentContext) throws TException {
    alter_partitions(getDefaultCatalog(conf), dbName, tblName, newParts, environmentContext);
  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName,
                               List<Partition> newParts,
                               EnvironmentContext environmentContext) throws TException {
    client.alter_partitions_with_environment_context(prependCatalogToDbName(catName, dbName, conf),
        tblName, newParts, environmentContext);
  }

  @Override
  public void alterDatabase(String dbName, Database db) throws TException {
    alterDatabase(getDefaultCatalog(conf), dbName, db);
  }

  @Override
  public void alterDatabase(String catName, String dbName, Database newDb) throws TException {
    client.alter_database(prependCatalogToDbName(catName, dbName, conf), newDb);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws TException {
    return getFields(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public List<FieldSchema> getFields(String catName, String db, String tableName)
      throws TException {
    List<FieldSchema> fields = client.get_fields(prependCatalogToDbName(catName, db, conf), tableName);
    return deepCopyFieldSchemas(fields);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req) throws TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_primary_keys(req).getPrimaryKeys();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws MetaException,
    NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_foreign_keys(req).getForeignKeys();
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest req)
    throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_unique_constraints(req).getUniqueConstraints();
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest req)
    throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_not_null_constraints(req).getNotNullConstraints();
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_default_constraints(req).getDefaultConstraints();
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    if (!req.isSetCatName()) {
      req.setCatName(getDefaultCatalog(conf));
    }
    return client.get_check_constraints(req).getCheckConstraints();
  }

  /** {@inheritDoc} */
  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws TException {
    if (!statsObj.getStatsDesc().isSetCatName()) {
      statsObj.getStatsDesc().setCatName(getDefaultCatalog(conf));
    }
    return client.update_table_column_statistics(statsObj);
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws TException {
    if (!statsObj.getStatsDesc().isSetCatName()) {
      statsObj.getStatsDesc().setCatName(getDefaultCatalog(conf));
    }
    return client.update_partition_column_statistics(statsObj);
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
    String defaultCat = getDefaultCatalog(conf);
    for (ColumnStatistics stats : request.getColStats()) {
      if (!stats.getStatsDesc().isSetCatName()) {
        stats.getStatsDesc().setCatName(defaultCat);
      }
    }
    return client.set_aggr_stats_for(request);
  }

  @Override
  public void flushCache() {
    try {
      client.flushCache();
    } catch (TException e) {
      // Not much we can do about it honestly
      LOG.warn("Got error flushing the cache", e);
    }
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
                                                            String tableName,
                                                            List<String> colNames) throws TException {
    TableStatsRequest rqst = new TableStatsRequest(dbName, tableName, colNames);
    rqst.setCatName(catName);
    return client.get_table_statistics_req(rqst).getTableStats();
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames)
          throws TException {
    return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partNames, colNames);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName, List<String> partNames,
      List<String> colNames) throws TException {
    PartitionsStatsRequest rqst = new PartitionsStatsRequest(dbName, tableName, colNames,
        partNames);
    rqst.setCatName(catName);
    return client.get_partitions_statistics_req(rqst).getPartStats();
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws TException {
    return deletePartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partName,
        colName);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
                                                 String partName, String colName)
      throws TException {
    return client.delete_partition_column_statistics(prependCatalogToDbName(catName, dbName, conf),
        tableName, partName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws TException {
    return deleteTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
                                             String colName) throws TException {
    return client.delete_table_column_statistics(prependCatalogToDbName(catName, dbName, conf),
        tableName, colName);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName) throws TException {
    return getSchema(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String catName, String db, String tableName) throws TException {
    EnvironmentContext envCxt = null;
    String addedJars = MetastoreConf.getVar(conf, ConfVars.ADDED_JARS);
    if(org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
      Map<String, String> props = new HashMap<>();
      props.put("hive.added.jars.path", addedJars);
      envCxt = new EnvironmentContext(props);
    }

    List<FieldSchema> fields = client.get_schema_with_environment_context(prependCatalogToDbName(
        catName, db, conf), tableName, envCxt);
    return deepCopyFieldSchemas(fields);
  }

  @Override
  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return client.get_config_value(name, defaultValue);
  }

  @Override
  public Partition getPartition(String db, String tableName, String partName) throws TException {
    return getPartition(getDefaultCatalog(conf), db, tableName, partName);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws TException {
    Partition p = client.get_partition_by_name(prependCatalogToDbName(catName, dbName, conf), tblName,
        name);
    return deepCopy(filterHook.filterPartition(p));
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartitionByName(dbName, tableName, partName, null);
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    Partition p = client.append_partition_by_name_with_environment_context(dbName, tableName,
        partName, envContext);
    return deepCopy(p);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return dropPartitionByName(dbName, tableName, partName, deleteData, null);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_partition_by_name_with_environment_context(dbName, tableName, partName,
        deleteData, envContext);
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return client.partition_name_to_vals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException{
    return client.partition_name_to_spec(name);
  }

  /**
   * @param partition
   * @return
   */
  protected Partition deepCopy(Partition partition) {
    Partition copy = null;
    if (partition != null) {
      copy = new Partition(partition);
    }
    return copy;
  }

  private Database deepCopy(Database database) {
    Database copy = null;
    if (database != null) {
      copy = new Database(database);
    }
    return copy;
  }

  protected Table deepCopy(Table table) {
    Table copy = null;
    if (table != null) {
      copy = new Table(table);
    }
    return copy;
  }

  private Type deepCopy(Type type) {
    Type copy = null;
    if (type != null) {
      copy = new Type(type);
    }
    return copy;
  }

  private FieldSchema deepCopy(FieldSchema schema) {
    FieldSchema copy = null;
    if (schema != null) {
      copy = new FieldSchema(schema);
    }
    return copy;
  }

  private Function deepCopy(Function func) {
    Function copy = null;
    if (func != null) {
      copy = new Function(func);
    }
    return copy;
  }

  protected PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
    PrincipalPrivilegeSet copy = null;
    if (pps != null) {
      copy = new PrincipalPrivilegeSet(pps);
    }
    return copy;
  }

  private List<Partition> deepCopyPartitions(List<Partition> partitions) {
    return deepCopyPartitions(partitions, null);
  }

  private List<Partition> deepCopyPartitions(
      Collection<Partition> src, List<Partition> dest) {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (Partition part : src) {
      dest.add(deepCopy(part));
    }
    return dest;
  }

  private List<Table> deepCopyTables(List<Table> tables) {
    List<Table> copy = null;
    if (tables != null) {
      copy = new ArrayList<Table>();
      for (Table tab : tables) {
        copy.add(deepCopy(tab));
      }
    }
    return copy;
  }

  protected List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
    List<FieldSchema> copy = null;
    if (schemas != null) {
      copy = new ArrayList<FieldSchema>();
      for (FieldSchema schema : schemas) {
        copy.add(deepCopy(schema));
      }
    }
    return copy;
  }

  @Override
  public boolean grant_role(String roleName, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, TException {
    GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
    req.setRequestType(GrantRevokeType.GRANT);
    req.setRoleName(roleName);
    req.setPrincipalName(userName);
    req.setPrincipalType(principalType);
    req.setGrantor(grantor);
    req.setGrantorType(grantorType);
    req.setGrantOption(grantOption);
    GrantRevokeRoleResponse res = client.grant_revoke_role(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean create_role(Role role)
      throws MetaException, TException {
    return client.create_role(role);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return client.drop_role(roleName);
  }

  @Override
  public List<Role> list_roles(String principalName,
      PrincipalType principalType) throws MetaException, TException {
    return client.list_roles(principalName, principalType);
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return client.get_role_names();
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest req)
      throws MetaException, TException {
    return client.get_principals_in_role(req);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
    return client.get_role_grants_for_principal(getRolePrincReq);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges)
      throws MetaException, TException {
    String defaultCat = getDefaultCatalog(conf);
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      if (!priv.getHiveObject().isSetCatName()) {
        priv.getHiveObject().setCatName(defaultCat);
      }
    }
    GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
    req.setRequestType(GrantRevokeType.GRANT);
    req.setPrivileges(privileges);
    GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean revoke_role(String roleName, String userName,
      PrincipalType principalType, boolean grantOption) throws MetaException, TException {
    GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
    req.setRequestType(GrantRevokeType.REVOKE);
    req.setRoleName(roleName);
    req.setPrincipalName(userName);
    req.setPrincipalType(principalType);
    req.setGrantOption(grantOption);
    GrantRevokeRoleResponse res = client.grant_revoke_role(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException,
      TException {
    String defaultCat = getDefaultCatalog(conf);
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      if (!priv.getHiveObject().isSetCatName()) {
        priv.getHiveObject().setCatName(defaultCat);
      }
    }
    GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
    req.setRequestType(GrantRevokeType.REVOKE);
    req.setPrivileges(privileges);
    req.setRevokeGrantOption(grantOption);
    GrantRevokePrivilegeResponse res = client.grant_revoke_privileges(req);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
      PrivilegeBag grantPrivileges) throws MetaException,
      TException {
    String defaultCat = getDefaultCatalog(conf);
    objToRefresh.setCatName(defaultCat);

    if (grantPrivileges.getPrivileges() != null) {
      for (HiveObjectPrivilege priv : grantPrivileges.getPrivileges()) {
        if (!priv.getHiveObject().isSetCatName()) {
          priv.getHiveObject().setCatName(defaultCat);
        }
      }
    }
    GrantRevokePrivilegeRequest grantReq = new GrantRevokePrivilegeRequest();
    grantReq.setRequestType(GrantRevokeType.GRANT);
    grantReq.setPrivileges(grantPrivileges);

    GrantRevokePrivilegeResponse res = client.refresh_privileges(objToRefresh, authorizer, grantReq);
    if (!res.isSetSuccess()) {
      throw new MetaException("GrantRevokePrivilegeResponse missing success field");
    }
    return res.isSuccess();
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException,
      TException {
    if (!hiveObject.isSetCatName()) {
      hiveObject.setCatName(getDefaultCatalog(conf));
    }
    return client.get_privilege_set(hiveObject, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principalName,
      PrincipalType principalType, HiveObjectRef hiveObject)
      throws MetaException, TException {
    if (!hiveObject.isSetCatName()) {
      hiveObject.setCatName(getDefaultCatalog(conf));
    }
    return client.list_privileges(principalName, principalType, hiveObject);
  }

  public String getDelegationToken(String renewerKerberosPrincipalName) throws
  MetaException, TException, IOException {
    //a convenience method that makes the intended owner for the delegation
    //token request the current user
    String owner = SecurityUtils.getUser();
    return getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws
  MetaException, TException {
    // This is expected to be a no-op, so we will return null when we use local metastore.
    if (localMetaStore) {
      return null;
    }
    return client.get_delegation_token(owner, renewerKerberosPrincipalName);
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    if (localMetaStore) {
      return 0;
    }
    return client.renew_delegation_token(tokenStrForm);

  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    if (localMetaStore) {
      return;
    }
    client.cancel_delegation_token(tokenStrForm);
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
     return client.add_token(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return client.remove_token(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return client.get_token(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return client.get_all_token_identifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return client.add_master_key(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException, TException {
    client.update_master_key(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return client.remove_master_key(keySeq);
  }

  @Override
  public String[] getMasterKeys() throws TException {
    List<String> keyList = client.get_master_keys();
    return keyList.toArray(new String[keyList.size()]);
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return TxnUtils.createValidReadTxnList(client.get_open_txns(), 0);
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return TxnUtils.createValidReadTxnList(client.get_open_txns(), currentTxn);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName), null);
    GetValidWriteIdsResponse validWriteIds = client.get_valid_write_ids(rqst);
    return TxnUtils.createValidReaderWriteIdList(validWriteIds.getTblValidWriteIds().get(0));
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(
      List<String> tablesList, String validTxnList) throws TException {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tablesList, validTxnList);
    return client.get_valid_write_ids(rqst).getTblValidWriteIds();
  }

  @Override
  public long openTxn(String user) throws TException {
    OpenTxnsResponse txns = openTxnsIntr(user, 1, null, null);
    return txns.getTxn_ids().get(0);
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
    // As this is called from replication task, the user is the user who has fired the repl command.
    // This is required for standalone metastore authentication.
    OpenTxnsResponse txns = openTxnsIntr(user, srcTxnIds.size(), replPolicy, srcTxnIds);
    return txns.getTxn_ids();
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return openTxnsIntr(user, numTxns, null, null);
  }

  private OpenTxnsResponse openTxnsIntr(String user, int numTxns, String replPolicy,
                                        List<Long> srcTxnIds) throws TException {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
    OpenTxnRequest rqst = new OpenTxnRequest(numTxns, user, hostname);
    if (replPolicy != null) {
      assert  srcTxnIds != null;
      assert numTxns == srcTxnIds.size();
      // need to set this only for replication tasks
      rqst.setReplPolicy(replPolicy);
      rqst.setReplSrcTxnIds(srcTxnIds);
    } else {
      assert srcTxnIds == null;
    }
    return client.open_txns(rqst);
  }

  @Override
  public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    client.abort_txn(new AbortTxnRequest(txnid));
  }

  @Override
  public void replRollbackTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TException {
    AbortTxnRequest rqst = new AbortTxnRequest(srcTxnId);
    rqst.setReplPolicy(replPolicy);
    client.abort_txn(rqst);
  }

  @Override
  public void commitTxn(long txnid)
          throws NoSuchTxnException, TxnAbortedException, TException {
    client.commit_txn(new CommitTxnRequest(txnid));
  }

  @Override
  public void replCommitTxn(long srcTxnId, String replPolicy)
          throws NoSuchTxnException, TxnAbortedException, TException {
    CommitTxnRequest rqst = new CommitTxnRequest(srcTxnId);
    rqst.setReplPolicy(replPolicy);
    client.commit_txn(rqst);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return client.get_open_txns_info();
  }

  @Override
  public void abortTxns(List<Long> txnids) throws NoSuchTxnException, TException {
    client.abort_txns(new AbortTxnsRequest(txnids));
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
          throws TException {
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      LOG.error("Unable to resolve current user name " + e.getMessage());
      throw new RuntimeException(e);
    }

    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }

    ReplTblWriteIdStateRequest rqst
            = new ReplTblWriteIdStateRequest(validWriteIdList, user, hostName, dbName, tableName);
    if (partNames != null) {
      rqst.setPartNames(partNames);
    }
    client.repl_tbl_writeid_state(rqst);
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    return allocateTableWriteIdsBatch(Collections.singletonList(txnId), dbName, tableName).get(0).getWriteId();
  }

  @Override
  public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
          throws TException {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    rqst.setTxnIds(txnIds);
    return allocateTableWriteIdsBatchIntr(rqst);
  }

  @Override
  public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName,
                                 String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    rqst.setReplPolicy(replPolicy);
    rqst.setSrcTxnToWriteIdList(srcTxnToWriteIdList);
    return allocateTableWriteIdsBatchIntr(rqst);
  }

  private List<TxnToWriteId> allocateTableWriteIdsBatchIntr(AllocateTableWriteIdsRequest rqst) throws TException {
    return client.allocate_table_write_ids(rqst).getTxnToWriteIds();
  }

  @Override
  public LockResponse lock(LockRequest request)
      throws NoSuchTxnException, TxnAbortedException, TException {
    return client.lock(request);
  }

  @Override
  public LockResponse checkLock(long lockid)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException,
      TException {
    return client.check_lock(new CheckLockRequest(lockid));
  }

  @Override
  public void unlock(long lockid)
      throws NoSuchLockException, TxnOpenException, TException {
    client.unlock(new UnlockRequest(lockid));
  }

  @Override
  @Deprecated
  public ShowLocksResponse showLocks() throws TException {
    return client.show_locks(new ShowLocksRequest());
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
    return client.show_locks(request);
  }

  @Override
  public void heartbeat(long txnid, long lockid)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException,
      TException {
    HeartbeatRequest hb = new HeartbeatRequest();
    hb.setLockid(lockid);
    hb.setTxnid(txnid);
    client.heartbeat(hb);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max)
    throws NoSuchTxnException, TxnAbortedException, TException {
    HeartbeatTxnRangeRequest rqst = new HeartbeatTxnRangeRequest(min, max);
    return client.heartbeat_txn_range(rqst);
  }

  @Override
  @Deprecated
  public void compact(String dbname, String tableName, String partitionName,  CompactionType type)
      throws TException {
    CompactionRequest cr = new CompactionRequest();
    if (dbname == null) {
      cr.setDbname(DEFAULT_DATABASE_NAME);
    } else {
      cr.setDbname(dbname);
    }
    cr.setTablename(tableName);
    if (partitionName != null) {
      cr.setPartitionname(partitionName);
    }
    cr.setType(type);
    client.compact(cr);
  }
  @Deprecated
  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type,
                      Map<String, String> tblproperties) throws TException {
    compact2(dbname, tableName, partitionName, type, tblproperties);
  }

  @Override
  public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
                      Map<String, String> tblproperties) throws TException {
    CompactionRequest cr = new CompactionRequest();
    if (dbname == null) {
      cr.setDbname(DEFAULT_DATABASE_NAME);
    } else {
      cr.setDbname(dbname);
    }
    cr.setTablename(tableName);
    if (partitionName != null) {
      cr.setPartitionname(partitionName);
    }
    cr.setType(type);
    cr.setProperties(tblproperties);
    return client.compact2(cr);
  }
  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return client.show_compact(new ShowCompactRequest());
  }

  @Deprecated
  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                   List<String> partNames) throws TException {
    client.add_dynamic_partitions(new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames));
  }
  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                   List<String> partNames, DataOperationType operationType) throws TException {
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames);
    adp.setOperationType(operationType);
    client.add_dynamic_partitions(adp);
  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {
    boolean failed = true;
    HiveMetaHook hook = getHook(table);
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
    try {
      hiveMetaHook.commitInsertTable(table, overwrite);
      failed = false;
    }
    finally {
      if (failed) {
        hiveMetaHook.rollbackInsertTable(table, overwrite);
      }
    }
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
                                                       NotificationFilter filter) throws TException {
    NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);
    rqst.setMaxEvents(maxEvents);
    NotificationEventResponse rsp = client.get_next_notification(rqst);
    LOG.debug("Got back " + rsp.getEventsSize() + " events");
    NotificationEventResponse filtered = new NotificationEventResponse();
    if (rsp != null && rsp.getEvents() != null) {
      long nextEventId = lastEventId + 1;
      for (NotificationEvent e : rsp.getEvents()) {
        if (e.getEventId() != nextEventId) {
          LOG.error("Requested events are found missing in NOTIFICATION_LOG table. Expected: {}, Actual: {}. "
                  + "Probably, cleaner would've cleaned it up. "
                  + "Try setting higher value for hive.metastore.event.db.listener.timetolive. "
                  + "Also, bootstrap the system again to get back the consistent replicated state.",
                  nextEventId, e.getEventId());
          throw new IllegalStateException(REPL_EVENTS_MISSING_IN_METASTORE);
        }
        if ((filter != null) && filter.accept(e)) {
          filtered.addToEvents(e);
        }
        nextEventId++;
      }
    }
    return (filter != null) ? filtered : rsp;
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return client.get_current_notificationEventId();
  }

  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
          throws TException {
    if (!rqst.isSetCatName()) {
      rqst.setCatName(getDefaultCatalog(conf));
    }
    return client.get_notification_events_count(rqst);
  }

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
    if (!rqst.isSetCatName()) {
      rqst.setCatName(getDefaultCatalog(conf));
    }
    return client.fire_listener_event(rqst);
  }

  /**
   * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
   * This may be used by multi-threaded applications until we have
   * fixed all reentrancy bugs.
   *
   * @param client unsynchronized client
   *
   * @return synchronized client
   */
  public static IMetaStoreClient newSynchronizedClient(
      IMetaStoreClient client) {
    return (IMetaStoreClient) Proxy.newProxyInstance(
      HiveMetaStoreClient.class.getClassLoader(),
      new Class [] { IMetaStoreClient.class },
      new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final IMetaStoreClient client;

    SynchronizedHandler(IMetaStoreClient client) {
      this.client = client;
    }

    @Override
    public synchronized Object invoke(Object proxy, Method method, Object [] args)
        throws Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    }
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String,String> partKVs, PartitionEventType eventType)
      throws TException {
    markPartitionForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public void markPartitionForEvent(String catName, String db_name, String tbl_name,
                                    Map<String, String> partKVs,
                                    PartitionEventType eventType) throws TException {
    client.markPartitionForEvent(prependCatalogToDbName(catName, db_name, conf), tbl_name, partKVs,
        eventType);

  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String,String> partKVs, PartitionEventType eventType)
      throws TException {
    return isPartitionMarkedForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name,
                                           Map<String, String> partKVs,
                                           PartitionEventType eventType) throws TException {
    return client.isPartitionMarkedForEvent(prependCatalogToDbName(catName, db_name, conf), tbl_name,
        partKVs, eventType);
  }

  @Override
  public void createFunction(Function func) throws TException {
    if (!func.isSetCatName()) {
      func.setCatName(getDefaultCatalog(conf));
    }
    client.create_function(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws TException {
    alterFunction(getDefaultCatalog(conf), dbName, funcName, newFunction);
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName,
                            Function newFunction) throws TException {
    client.alter_function(prependCatalogToDbName(catName, dbName, conf), funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws TException {
    dropFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName) throws TException {
    client.drop_function(prependCatalogToDbName(catName, dbName, conf), funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws TException {
    return getFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws TException {
    return deepCopy(client.get_function(prependCatalogToDbName(catName, dbName, conf), funcName));
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws TException {
    return getFunctions(getDefaultCatalog(conf), dbName, pattern);
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern) throws TException {
    return client.get_functions(prependCatalogToDbName(catName, dbName, conf), pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws TException {
    return client.get_all_functions();
  }

  protected void create_table_with_environment_context(Table tbl, EnvironmentContext envContext)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    client.create_table_with_environment_context(tbl, envContext);
  }

  protected void drop_table_with_environment_context(String catName, String dbname, String name,
      boolean deleteData, EnvironmentContext envContext) throws TException {
    client.drop_table_with_environment_context(prependCatalogToDbName(catName, dbname, conf),
        name, deleteData, envContext);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName,
    List<String> colNames, List<String> partNames) throws NoSuchObjectException, MetaException, TException {
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames, partNames);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
                                      List<String> colNames, List<String> partNames) throws TException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
      return new AggrStats(new ArrayList<>(),0); // Nothing to aggregate
    }
    PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
    req.setCatName(catName);
    return client.get_aggr_stats_for(req);
  }

  @Override
  public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(
      final List<Long> fileIds) throws TException {
    return new MetastoreMapIterable<Long, ByteBuffer>() {
      private int listIndex = 0;
      @Override
      protected Map<Long, ByteBuffer> fetchNextBatch() throws TException {
        if (listIndex == fileIds.size()) {
          return null;
        }
        int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
        List<Long> subList = fileIds.subList(listIndex, endIndex);
        GetFileMetadataResult resp = sendGetFileMetadataReq(subList);
        // TODO: we could remember if it's unsupported and stop sending calls; although, it might
        //       be a bad idea for HS2+standalone metastore that could be updated with support.
        //       Maybe we should just remember this for some time.
        if (!resp.isIsSupported()) {
          return null;
        }
        listIndex = endIndex;
        return resp.getMetadata();
      }
    };
  }

  private GetFileMetadataResult sendGetFileMetadataReq(List<Long> fileIds) throws TException {
    return client.get_file_metadata(new GetFileMetadataRequest(fileIds));
  }

  @Override
  public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      final List<Long> fileIds, final ByteBuffer sarg, final boolean doGetFooters)
          throws TException {
    return new MetastoreMapIterable<Long, MetadataPpdResult>() {
      private int listIndex = 0;
      @Override
      protected Map<Long, MetadataPpdResult> fetchNextBatch() throws TException {
        if (listIndex == fileIds.size()) {
          return null;
        }
        int endIndex = Math.min(listIndex + fileMetadataBatchSize, fileIds.size());
        List<Long> subList = fileIds.subList(listIndex, endIndex);
        GetFileMetadataByExprResult resp = sendGetFileMetadataBySargReq(
            sarg, subList, doGetFooters);
        if (!resp.isIsSupported()) {
          return null;
        }
        listIndex = endIndex;
        return resp.getMetadata();
      }
    };
  }

  private GetFileMetadataByExprResult sendGetFileMetadataBySargReq(
      ByteBuffer sarg, List<Long> fileIds, boolean doGetFooters) throws TException {
    GetFileMetadataByExprRequest req = new GetFileMetadataByExprRequest(fileIds, sarg);
    req.setDoGetFooters(doGetFooters); // No need to get footers
    return client.get_file_metadata_by_expr(req);
  }

  public static abstract class MetastoreMapIterable<K, V>
    implements Iterable<Entry<K, V>>, Iterator<Entry<K, V>> {
    private Iterator<Entry<K, V>> currentIter;

    protected abstract Map<K, V> fetchNextBatch() throws TException;

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      ensureCurrentBatch();
      return currentIter != null;
    }

    private void ensureCurrentBatch() {
      if (currentIter != null && currentIter.hasNext()) {
        return;
      }
      currentIter = null;
      Map<K, V> currentBatch;
      do {
        try {
          currentBatch = fetchNextBatch();
        } catch (TException ex) {
          throw new RuntimeException(ex);
        }
        if (currentBatch == null)
         {
          return; // No more data.
        }
      } while (currentBatch.isEmpty());
      currentIter = currentBatch.entrySet().iterator();
    }

    @Override
    public Entry<K, V> next() {
      ensureCurrentBatch();
      if (currentIter == null) {
        throw new NoSuchElementException();
      }
      return currentIter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {
    ClearFileMetadataRequest req = new ClearFileMetadataRequest();
    req.setFileIds(fileIds);
    client.clear_file_metadata(req);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    PutFileMetadataRequest req = new PutFileMetadataRequest();
    req.setFileIds(fileIds);
    req.setMetadata(metadata);
    client.put_file_metadata(req);
  }

  @Override
  public boolean isSameConfObj(Configuration c) {
    return conf == c;
  }

  @Override
  public boolean cacheFileMetadata(
      String dbName, String tableName, String partName, boolean allParts) throws TException {
    CacheFileMetadataRequest req = new CacheFileMetadataRequest();
    req.setDbName(dbName);
    req.setTblName(tableName);
    if (partName != null) {
      req.setPartName(partName);
    } else {
      req.setIsAllParts(allParts);
    }
    CacheFileMetadataResult result = client.cache_file_metadata(req);
    return result.isIsSupported();
  }

  @Override
  public String getMetastoreDbUuid() throws TException {
    return client.get_metastore_db_uuid();
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws InvalidObjectException, MetaException, TException {
    WMCreateResourcePlanRequest request = new WMCreateResourcePlanRequest();
    request.setResourcePlan(resourcePlan);
    request.setCopyFrom(copyFromName);
    client.create_resource_plan(request);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException, TException {
    WMGetResourcePlanRequest request = new WMGetResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    return client.get_resource_plan(request).getResourcePlan();
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans()
      throws NoSuchObjectException, MetaException, TException {
    WMGetAllResourcePlanRequest request = new WMGetAllResourcePlanRequest();
    return client.get_all_resource_plans(request).getResourcePlans();
  }

  @Override
  public void dropResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException, TException {
    WMDropResourcePlanRequest request = new WMDropResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    client.drop_resource_plan(request);
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterResourcePlanRequest request = new WMAlterResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setResourcePlan(resourcePlan);
    request.setIsEnableAndActivate(canActivateDisabled);
    request.setIsForceDeactivate(isForceDeactivate);
    request.setIsReplace(isReplace);
    WMAlterResourcePlanResponse resp = client.alter_resource_plan(request);
    return resp.isSetFullResourcePlan() ? resp.getFullResourcePlan() : null;
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
    return client.get_active_resource_plan(new WMGetActiveResourcePlanRequest()).getResourcePlan();
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMValidateResourcePlanRequest request = new WMValidateResourcePlanRequest();
    request.setResourcePlanName(resourcePlanName);
    return client.validate_resource_plan(request);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws InvalidObjectException, MetaException, TException {
    WMCreateTriggerRequest request = new WMCreateTriggerRequest();
    request.setTrigger(trigger);
    client.create_wm_trigger(request);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterTriggerRequest request = new WMAlterTriggerRequest();
    request.setTrigger(trigger);
    client.alter_wm_trigger(request);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, MetaException, TException {
    WMDropTriggerRequest request = new WMDropTriggerRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setTriggerName(triggerName);
    client.drop_wm_trigger(request);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan)
      throws NoSuchObjectException, MetaException, TException {
    WMGetTriggersForResourePlanRequest request = new WMGetTriggersForResourePlanRequest();
    request.setResourcePlanName(resourcePlan);
    return client.get_triggers_for_resourceplan(request).getTriggers();
  }

  @Override
  public void createWMPool(WMPool pool)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMCreatePoolRequest request = new WMCreatePoolRequest();
    request.setPool(pool);
    client.create_wm_pool(request);
  }

  @Override
  public void alterWMPool(WMNullablePool pool, String poolPath)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMAlterPoolRequest request = new WMAlterPoolRequest();
    request.setPool(pool);
    request.setPoolPath(poolPath);
    client.alter_wm_pool(request);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, MetaException, TException {
    WMDropPoolRequest request = new WMDropPoolRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setPoolPath(poolPath);
    client.drop_wm_pool(request);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    WMCreateOrUpdateMappingRequest request = new WMCreateOrUpdateMappingRequest();
    request.setMapping(mapping);
    request.setUpdate(isUpdate);
    client.create_or_update_wm_mapping(request);
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, MetaException, TException {
    WMDropMappingRequest request = new WMDropMappingRequest();
    request.setMapping(mapping);
    client.drop_wm_mapping(request);
  }

  @Override
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop) throws AlreadyExistsException, NoSuchObjectException,
      InvalidObjectException, MetaException, TException {
    WMCreateOrDropTriggerToPoolMappingRequest request = new WMCreateOrDropTriggerToPoolMappingRequest();
    request.setResourcePlanName(resourcePlanName);
    request.setTriggerName(triggerName);
    request.setPoolPath(poolPath);
    request.setDrop(shouldDrop);
    client.create_or_drop_wm_trigger_to_pool_mapping(request);
  }

  @Override
  public void createISchema(ISchema schema) throws TException {
    if (!schema.isSetCatName()) {
      schema.setCatName(getDefaultCatalog(conf));
    }
    client.create_ischema(schema);
  }

  @Override
  public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
    client.alter_ischema(new AlterISchemaRequest(new ISchemaName(catName, dbName, schemaName), newSchema));
  }

  @Override
  public ISchema getISchema(String catName, String dbName, String name) throws TException {
    return client.get_ischema(new ISchemaName(catName, dbName, name));
  }

  @Override
  public void dropISchema(String catName, String dbName, String name) throws TException {
    client.drop_ischema(new ISchemaName(catName, dbName, name));
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
    if (!schemaVersion.getSchema().isSetCatName()) {
      schemaVersion.getSchema().setCatName(getDefaultCatalog(conf));
    }
    client.add_schema_version(schemaVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    return client.get_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
  }

  @Override
  public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
    return client.get_schema_latest_version(new ISchemaName(catName, dbName, schemaName));
  }

  @Override
  public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
    return client.get_schema_all_versions(new ISchemaName(catName, dbName, schemaName));
  }

  @Override
  public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    client.drop_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
  }

  @Override
  public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
    return client.get_schemas_by_cols(rqst);
  }

  @Override
  public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName)
      throws TException {
    client.map_schema_version_to_serde(new MapSchemaVersionToSerdeRequest(
        new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version), serdeName));
  }

  @Override
  public void setSchemaVersionState(String catName, String dbName, String schemaName, int version, SchemaVersionState state)
      throws TException {
    client.set_schema_version_state(new SetSchemaVersionStateRequest(new SchemaVersionDescriptor(
        new ISchemaName(catName, dbName, schemaName), version), state));
  }

  @Override
  public void addSerDe(SerDeInfo serDeInfo) throws TException {
    client.add_serde(serDeInfo);
  }

  @Override
  public SerDeInfo getSerDe(String serDeName) throws TException {
    return client.get_serde(new GetSerdeRequest(serDeName));
  }

  private short shrinkMaxtoShort(int max) {
    if (max < 0) {
      return -1;
    } else if (max <= Short.MAX_VALUE) {
      return (short)max;
    } else {
      return Short.MAX_VALUE;
    }
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return client.get_lock_materialization_rebuild(dbName, tableName, txnId);
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return client.heartbeat_lock_materialization_rebuild(dbName, tableName, txnId);
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws TException {
    client.add_runtime_stats(stat);
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
    GetRuntimeStatsRequest req = new GetRuntimeStatsRequest();
    req.setMaxWeight(maxWeight);
    req.setMaxCreateTime(maxCreateTime);
    return client.get_runtime_stats(req);
  }
}
