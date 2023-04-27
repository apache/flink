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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;

/** Sub-class of SessionState to meet our needs. */
public class HiveSessionState extends SessionState {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSessionState.class);

    private static final Method setCurrentTSMethod =
            HiveReflectionUtils.tryGetMethod(
                    SessionState.class, "setupQueryCurrentTimestamp", new Class[0]);
    private static final Method getCurrentTSMethod =
            HiveReflectionUtils.tryGetMethod(
                    SessionState.class, "getQueryCurrentTimestamp", new Class[0]);

    private static final Class registryClz;
    private static final Method getRegistry;
    private static final Method clearRegistry;
    private static final Method closeRegistryLoaders;

    private Timestamp hiveParserCurrentTS;

    static {
        registryClz = HiveReflectionUtils.tryGetClass("org.apache.hadoop.hive.ql.exec.Registry");
        if (registryClz != null) {
            getRegistry =
                    HiveReflectionUtils.tryGetMethod(
                            SessionState.class, "getRegistry", new Class[0]);
            clearRegistry = HiveReflectionUtils.tryGetMethod(registryClz, "clear", new Class[0]);
            closeRegistryLoaders =
                    HiveReflectionUtils.tryGetMethod(registryClz, "closeCUDFLoaders", new Class[0]);
        } else {
            getRegistry = null;
            clearRegistry = null;
            closeRegistryLoaders = null;
        }
    }

    private final ClassLoader originContextLoader;
    private final ClassLoader hiveLoader;

    public HiveSessionState(HiveConf conf, ClassLoader contextLoader) {
        super(conf);
        this.originContextLoader = contextLoader;
        this.hiveLoader = getConf().getClassLoader();
        // added jars are handled by context class loader, so we always use it as the session
        // class loader
        getConf().setClassLoader(contextLoader);
    }

    @Override
    public void close() throws IOException {
        clearSessionRegistry();
        if (getTxnMgr() != null) {
            getTxnMgr().closeTxnManager();
        }
        // close the classloader created in hive
        JavaUtils.closeClassLoadersTo(hiveLoader, originContextLoader);
        File resourceDir = new File(getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
        LOG.debug("Removing resource dir " + resourceDir);
        FileUtils.deleteDirectoryQuietly(resourceDir);
        Hive.closeCurrent();
        detachSession();
    }

    public void setHiveParserCurrentTSCurrentTS(Timestamp hiveParserCurrentTS) {
        this.hiveParserCurrentTS = hiveParserCurrentTS;
    }

    public Timestamp getHiveParserCurrentTS() {
        return hiveParserCurrentTS;
    }

    private void clearSessionRegistry() {
        if (getRegistry != null) {
            try {
                Object registry = getRegistry.invoke(this);
                if (registry != null) {
                    clearRegistry.invoke(registry);
                    closeRegistryLoaders.invoke(registry);
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                LOG.warn("Failed to clear session registry", e);
            }
        }
    }

    public static void startSessionState(HiveConf hiveConf, CatalogRegistry catalogRegistry) {
        final ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        try {
            HiveSessionState sessionState = new HiveSessionState(hiveConf, contextCL);
            sessionState.initTxnMgr(hiveConf);
            sessionState.setCurrentDatabase(catalogRegistry.getCurrentDatabase());
            // some Hive functions needs the timestamp
            setCurrentTimestamp(sessionState);
            SessionState.setCurrentSessionState(sessionState);
        } catch (LockException e) {
            throw new FlinkHiveException("Failed to init SessionState", e);
        } finally {
            // don't let SessionState mess up with our context classloader
            Thread.currentThread().setContextClassLoader(contextCL);
        }
    }

    private static void setCurrentTimestamp(HiveSessionState sessionState) {
        if (setCurrentTSMethod != null) {
            try {
                setCurrentTSMethod.invoke(sessionState);
                Object currentTs = getCurrentTSMethod.invoke(sessionState);
                if (currentTs instanceof Instant) {
                    sessionState.setHiveParserCurrentTSCurrentTS(
                            Timestamp.from((Instant) currentTs));
                } else {
                    sessionState.setHiveParserCurrentTSCurrentTS((Timestamp) currentTs);
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new FlinkHiveException("Failed to set current timestamp for session", e);
            }
        } else {
            sessionState.setHiveParserCurrentTSCurrentTS(new Timestamp(System.currentTimeMillis()));
        }
    }

    public static void clearSessionState() {
        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
            try {
                sessionState.close();
            } catch (Exception e) {
                LOG.warn("Error closing SessionState", e);
            }
        }
    }
}
