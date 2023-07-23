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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Map;

/**
 * A factory to create configured catalog store instances based on string-based properties. See also
 * {@link Factory} for more information.
 *
 * <p>This factory is specifically designed for the Flink SQL gateway scenario, where different
 * catalog stores need to be created for different sessions.
 *
 * <p>If the CatalogStore is implemented using JDBC, this factory can be used to create a JDBC
 * connection pool in the open method. This connection pool can then be reused for subsequent
 * catalog store creations.
 *
 * <p>The following examples implementation of CatalogStoreFactory using jdbc.
 *
 * <pre>{@code
 * public class JdbcCatalogStore implements CatalogStore {
 *
 *     private JdbcConnectionPool jdbcConnectionPool;
 *     public JdbcCatalogStore(JdbcConnectionPool jdbcConnectionPool) {
 *         this.jdbcConnectionPool = jdbcConnectionPool;
 *     }
 *     ...
 * }
 *
 * public class JdbcCatalogStoreFactory implements CatalogStoreFactory {
 *
 *     private JdbcConnectionPool jdbcConnectionPool;
 *
 *     @Override
 *     public CatalogStore createCatalogStore(Context context) {
 *         return new JdbcCatalogStore(jdbcConnectionPool);
 *     }
 *
 *     @Override
 *     public void open(Context context) throws CatalogException {
 *         // initialize the thread pool using options from context
 *         jdbcConnectionPool = initializeJdbcConnectionPool(context);
 *     }
 *
 *     @Override
 *     public void close() {
 *         // release the connection thread pool.
 *         releaseConnectionPool(jdbcConnectionPool);
 *     }
 *     ...
 * }
 * }</pre>
 *
 * <p>The usage of the Flink SQL gateway is as follows. It's just an example and may not be the
 * final implementation.
 *
 * <pre>{@code
 *  // initialize CatalogStoreFactory when initialize the SessionManager
 *  public class SessionManagerImpl implements SessionManager {
 *      public SessionManagerImpl(DefaultContext defaultContext) {
 *        this.catalogStoreFactory = createCatalogStore();
 *      }
 *
 *      @Override
 *      public void start() {
 *          // initialize the CatalogStoreFactory
 *          this.catalogStoreFactory(buildCatalogStoreContext());
 *      }
 *
 *      @Override
 *      public synchronized Session openSession(SessionEnvironment environment) {
 *          // Create a new catalog store for this session.
 *          CatalogStore catalogStore = this.catalogStoreFactory.createCatalogStore(buildCatalogStoreContext());
 *
 *          // Create a new CatalogManager using catalog store.
 *      }
 *
 *      @Override
 *      public void stop() {
 *          // Close the CatalogStoreFactory when stopping the SessionManager.
 *          this.catalogStoreFactory.close();
 *      }
 * }
 * }</pre>
 */
@PublicEvolving
public interface CatalogStoreFactory extends Factory {

    /** Creates a {@link CatalogStore} instance from context information. */
    CatalogStore createCatalogStore();

    /**
     * Initialize the CatalogStoreFactory.
     *
     * <p>For the use case of Flink SQL gateway, the open method will be called when starting
     * SessionManager. It initializes common resources, such as a connection pool, for various
     * catalog stores.
     */
    void open(Context context) throws CatalogException;

    /**
     * Close the CatalogStoreFactory.
     *
     * <p>For the use case of Flink SQL gateway, the close method will be called when closing
     * SessionManager. It releases common resources, such as connection pool, after closing all
     * catalog stores.
     */
    void close() throws CatalogException;

    /** Context provided when a catalog store is created. */
    @PublicEvolving
    interface Context {

        /**
         * Returns the options with which the catalog store is created.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getOptions();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();
    }
}
