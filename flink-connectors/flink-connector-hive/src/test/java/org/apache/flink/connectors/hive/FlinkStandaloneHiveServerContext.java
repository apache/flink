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

package org.apache.flink.connectors.hive;

import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HADOOPBIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEHISTORYFILELOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEMETADATAONLYQUERIES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEOPTINDEXFILTER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESKEWJOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSAUTOGATHER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CBO_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALSCRATCHDIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_COLUMNS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_CONSTRAINTS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_TABLES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;

/**
 * HiveServerContext used by FlinkStandaloneHiveRunner.
 */
public class FlinkStandaloneHiveServerContext implements HiveServerContext {

	private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStandaloneHiveServerContext.class);

	private HiveConf hiveConf = new HiveConf();

	private final TemporaryFolder basedir;
	private final HiveRunnerConfig hiveRunnerConfig;
	private boolean inited = false;
	private final int hmsPort;

	FlinkStandaloneHiveServerContext(TemporaryFolder basedir, HiveRunnerConfig hiveRunnerConfig, int hmsPort) {
		this.basedir = basedir;
		this.hiveRunnerConfig = hiveRunnerConfig;
		this.hmsPort = hmsPort;
	}

	private String toHmsURI() {
		return "thrift://localhost:" + hmsPort;
	}

	@Override
	public final void init() {
		if (!inited) {

			configureMiscHiveSettings(hiveConf);

			configureMetaStore(hiveConf);

			configureMrExecutionEngine(hiveConf);

			configureJavaSecurityRealm(hiveConf);

			configureSupportConcurrency(hiveConf);

			configureFileSystem(basedir, hiveConf);

			configureAssertionStatus(hiveConf);

			overrideHiveConf(hiveConf);
		}
		inited = true;
	}

	private void configureMiscHiveSettings(HiveConf hiveConf) {
		hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

		// Turn off CBO so we don't depend on calcite
		hiveConf.setBoolVar(HIVE_CBO_ENABLED, false);

		// Disable to get rid of clean up exception when stopping the Session.
		hiveConf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

		hiveConf.setVar(HADOOPBIN, "NO_BIN!");
	}

	private void overrideHiveConf(HiveConf hiveConf) {
		for (Map.Entry<String, String> hiveConfEntry : hiveRunnerConfig.getHiveConfSystemOverride().entrySet()) {
			hiveConf.set(hiveConfEntry.getKey(), hiveConfEntry.getValue());
		}
	}

	private void configureMrExecutionEngine(HiveConf conf) {

		/*
		 * Switch off all optimizers otherwise we didn't
		 * manage to contain the map reduction within this JVM.
		 */
		conf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
		conf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
		conf.setBoolVar(HIVEOPTINDEXFILTER, false);
		conf.setBoolVar(HIVECONVERTJOIN, false);
		conf.setBoolVar(HIVESKEWJOIN, false);

		// Defaults to a 1000 millis sleep in. We can speed up the tests a bit by setting this to 1 millis instead.
		// org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
		hiveConf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

		hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
	}

	private void configureJavaSecurityRealm(HiveConf hiveConf) {
		// These three properties gets rid of: 'Unable to load realm info from SCDynamicStore'
		// which seems to have a timeout of about 5 secs.
		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");
		System.setProperty("java.security.krb5.conf", "/dev/null");
	}

	private void configureAssertionStatus(HiveConf conf) {
		ClassLoader.getSystemClassLoader().setPackageAssertionStatus(
				"org.apache.hadoop.hive.serde2.objectinspector",
				false);
	}

	private void configureSupportConcurrency(HiveConf conf) {
		hiveConf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
	}

	private void configureMetaStore(HiveConf conf) {

		String jdbcDriver = org.apache.derby.jdbc.EmbeddedDriver.class.getName();
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// Set the Hive Metastore DB driver
		hiveConf.set("datanucleus.schema.autoCreateAll", "true");
		hiveConf.set("hive.metastore.schema.verification", "false");
		hiveConf.set("hive.metastore.uris", toHmsURI());
		// No pooling needed. This will save us a lot of threads
		hiveConf.set("datanucleus.connectionPoolingType", "None");

		conf.setBoolVar(METASTORE_VALIDATE_CONSTRAINTS, true);
		conf.setBoolVar(METASTORE_VALIDATE_COLUMNS, true);
		conf.setBoolVar(METASTORE_VALIDATE_TABLES, true);

		// disable authorization to avoid NPE
		conf.set(HIVE_AUTHORIZATION_MANAGER.varname,
				"org.apache.hive.hcatalog.storagehandler.DummyHCatAuthProvider");
	}

	private void configureFileSystem(TemporaryFolder basedir, HiveConf conf) {

		createAndSetFolderProperty(METASTOREWAREHOUSE, "warehouse", conf, basedir);
		createAndSetFolderProperty(SCRATCHDIR, "scratchdir", conf, basedir);
		createAndSetFolderProperty(LOCALSCRATCHDIR, "localscratchdir", conf, basedir);
		createAndSetFolderProperty(HIVEHISTORYFILELOC, "tmp", conf, basedir);

		conf.setBoolVar(HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);

		createAndSetFolderProperty("hadoop.tmp.dir", "hadooptmp", conf, basedir);
		createAndSetFolderProperty("test.log.dir", "logs", conf, basedir);
	}

	private File newFolder(TemporaryFolder basedir, String folder) {
		try {
			File newFolder = basedir.newFolder(folder);
			FileUtil.setPermission(newFolder, FsPermission.getDirDefault());
			return newFolder;
		} catch (IOException e) {
			throw new IllegalStateException("Failed to create tmp dir: " + e.getMessage(), e);
		}
	}

	public HiveConf getHiveConf() {
		return hiveConf;
	}

	@Override
	public TemporaryFolder getBaseDir() {
		return basedir;
	}

	private void createAndSetFolderProperty(
			HiveConf.ConfVars var, String folder, HiveConf conf,
			TemporaryFolder basedir) {
		conf.setVar(var, newFolder(basedir, folder).getAbsolutePath());
	}

	private void createAndSetFolderProperty(String key, String folder, HiveConf conf, TemporaryFolder basedir) {
		conf.set(key, newFolder(basedir, folder).getAbsolutePath());
	}
}
