/*
 * Copyright 2015 EURA NOVA.
 *
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

package org.apache.flink.parameterserver.impl.ignite;

import org.apache.flink.parameterserver.model.ParameterElement;
import org.apache.flink.parameterserver.model.ParameterServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.flink.parameterserver.model.ParameterServer} using Apache Ignite
 */
public class ParameterServerIgniteImpl implements ParameterServer {

	private final static Logger log = LoggerFactory.getLogger(ParameterServerIgniteImpl.class);

	public final static String CACHE_NAME = ParameterServerIgniteImpl.class.getSimpleName();

	public final static String GRID_NAME = "FLINK_PARAMETER_SERVER";

	public static CacheConfiguration<String, ParameterElement> getParameterCacheConfiguration() {
		CacheConfiguration<String, ParameterElement> parameterCacheCfg = new CacheConfiguration<String, ParameterElement>();
		parameterCacheCfg.setCacheMode(CacheMode.PARTITIONED);
		parameterCacheCfg.setName(CACHE_NAME + "_parameter");
//		parameterCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		return parameterCacheCfg;
	}

	public static CacheConfiguration<String, ParameterElement> getSharedCacheConfiguration() {
		CacheConfiguration<String, ParameterElement> sharedCacheCfg = new CacheConfiguration<String, ParameterElement>();
		sharedCacheCfg.setCacheMode(CacheMode.REPLICATED);
		sharedCacheCfg.setName(CACHE_NAME + "_SHARED");
		return sharedCacheCfg;
	}

	private static boolean isGridAlreadyStarted(String name) {
		for(Ignite n:Ignition.allGrids()){
			if (n.name().equals(name)) {
				return true;
			}
		}
		return false;
	}

	public static synchronized void prepareInstance(String name) {
		if(!isGridAlreadyStarted(name)) {
			try {
				CacheConfiguration<String, ParameterElement> parameterCacheCfg = getParameterCacheConfiguration();
				CacheConfiguration<String, ParameterElement> sharedCacheCfg =
						getSharedCacheConfiguration();

				IgniteConfiguration cfg1 = new IgniteConfiguration();
				cfg1.setGridName(name);
				cfg1.setPeerClassLoadingEnabled(true);
				cfg1.setCacheConfiguration(parameterCacheCfg, sharedCacheCfg);

				if (log.isInfoEnabled()) {
					log.info("Starting parameter server " + name);
				}
				Ignite ignite = Ignition.start(cfg1);

				IgniteCluster cluster;

				IgniteCache<String, ParameterElement> parameterCache = ignite.getOrCreateCache(parameterCacheCfg).withAsync();
				IgniteCache<String, ParameterElement> sharedCache = ignite.getOrCreateCache(sharedCacheCfg).withAsync();

				if (log.isDebugEnabled()) {
					log.debug("I hereby confirm that parameter cache is async enabled: "
							+ parameterCache.isAsync());
					log.debug("I hereby confirm that shared cache is async enabled: "
							+ sharedCache.isAsync());
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void shutDown() {
		log.info("Stopping the parameter server");
		Ignite ignite = Ignition.ignite(ParameterServerIgniteImpl.GRID_NAME);
		IgniteCluster cluster = ignite.cluster();
		cluster.forServers();
		cluster.stopNodes();

//		Ignition.stopAll(true);
		if(log.isInfoEnabled()) {
			log.info("Parameter server successfully stopped.");
		}
	}
}
