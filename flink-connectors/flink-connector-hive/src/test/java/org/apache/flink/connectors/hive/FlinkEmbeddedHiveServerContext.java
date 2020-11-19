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

import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

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
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALSCRATCHDIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_COLUMNS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_CONSTRAINTS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_TABLES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;

/**
 * HiveServerContext used by FlinkEmbeddedHiveRunner.
 */
public class FlinkEmbeddedHiveServerContext implements HiveServerContext {

	private final HiveConf hiveConf = new HiveConf();

	private final TemporaryFolder basedir;
	private final HiveRunnerConfig hiveRunnerConfig;
	private boolean inited = false;

	FlinkEmbeddedHiveServerContext(TemporaryFolder basedir, HiveRunnerConfig hiveRunnerConfig) {
		this.basedir = basedir;
		this.hiveRunnerConfig = hiveRunnerConfig;
	}

	@Override
	public final void init() {
		if (!inited) {

			configureMiscHiveSettings();

			configureMetaStore();

			configureMrExecutionEngine();

			configureJavaSecurityRealm();

			configureSupportConcurrency();

			configureFileSystem();

			configureAssertionStatus();

			overrideHiveConf();

			setHiveSitePath();
		}
		inited = true;
	}

	// Some Hive code may create HiveConf instances relying on the hive-site in classpath. Make sure such code can
	// read the configurations we set here.
	private void setHiveSitePath() {
		File hiveSite = new File(newFolder(basedir, "hive-conf"), "hive-site.xml");
		try (FileOutputStream outputStream = new FileOutputStream(hiveSite)) {
			hiveConf.writeXml(outputStream);
			HiveConf.setHiveSiteLocation(hiveSite.toURI().toURL());
		} catch (IOException e) {
			throw new RuntimeException("Failed to write hive-site.xml", e);
		}
	}

	private void configureMiscHiveSettings() {
		hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);

		// Turn off CBO so we don't depend on calcite
		hiveConf.setBoolVar(HIVE_CBO_ENABLED, false);

		// Disable to get rid of clean up exception when stopping the Session.
		hiveConf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);

		hiveConf.setVar(HADOOPBIN, "NO_BIN!");

		// To avoid https://issues.apache.org/jira/browse/HIVE-13185 when loading data into tables
		hiveConf.setBoolVar(HiveConf.ConfVars.HIVECHECKFILEFORMAT, false);
	}

	private void overrideHiveConf() {
		for (Map.Entry<String, String> hiveConfEntry : hiveRunnerConfig.getHiveConfSystemOverride().entrySet()) {
			hiveConf.set(hiveConfEntry.getKey(), hiveConfEntry.getValue());
		}
	}

	private void configureMrExecutionEngine() {

		/*
		 * Switch off all optimizers otherwise we didn't
		 * manage to contain the map reduction within this JVM.
		 */
		hiveConf.setBoolVar(HIVE_INFER_BUCKET_SORT, false);
		hiveConf.setBoolVar(HIVEMETADATAONLYQUERIES, false);
		hiveConf.setBoolVar(HIVEOPTINDEXFILTER, false);
		hiveConf.setBoolVar(HIVECONVERTJOIN, false);
		hiveConf.setBoolVar(HIVESKEWJOIN, false);

		// Defaults to a 1000 millis sleep in. We can speed up the tests a bit by setting this to 1 millis instead.
		// org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
		hiveConf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

		hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);
	}

	private void configureJavaSecurityRealm() {
		// These three properties gets rid of: 'Unable to load realm info from SCDynamicStore'
		// which seems to have a timeout of about 5 secs.
		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");
		System.setProperty("java.security.krb5.conf", "/dev/null");
	}

	private void configureAssertionStatus() {
		ClassLoader.getSystemClassLoader().setPackageAssertionStatus(
				"org.apache.hadoop.hive.serde2.objectinspector",
				false);
	}

	private void configureSupportConcurrency() {
		hiveConf.setBoolVar(HIVE_SUPPORT_CONCURRENCY, false);
	}

	private void configureMetaStore() {

		String jdbcDriver = org.apache.derby.jdbc.EmbeddedDriver.class.getName();
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// No pooling needed. This will save us a lot of threads
		// hive-2.1.1 doesn't allow 'none'...
		if (!HiveShimLoader.getHiveVersion().equals("2.1.1")) {
			hiveConf.set("datanucleus.connectionPoolingType", "None");
		}

		// set JDO configs
		String jdoConnectionURL = "jdbc:derby:memory:" + UUID.randomUUID().toString();
		hiveConf.setVar(METASTORECONNECTURLKEY, jdoConnectionURL + ";create=true");

		hiveConf.setBoolVar(METASTORE_VALIDATE_CONSTRAINTS, true);
		hiveConf.setBoolVar(METASTORE_VALIDATE_COLUMNS, true);
		hiveConf.setBoolVar(METASTORE_VALIDATE_TABLES, true);

		// disable authorization to avoid NPE
		hiveConf.set(HIVE_AUTHORIZATION_MANAGER.varname,
				"org.apache.hive.hcatalog.storagehandler.DummyHCatAuthProvider");

		// disable notification event poll
		hiveConf.set("hive.notification.event.poll.interval", "0s");
	}

	private void configureFileSystem() {

		createAndSetFolderProperty(METASTOREWAREHOUSE, "warehouse", hiveConf, basedir);
		createAndSetFolderProperty(SCRATCHDIR, "scratchdir", hiveConf, basedir);
		createAndSetFolderProperty(LOCALSCRATCHDIR, "localscratchdir", hiveConf, basedir);
		createAndSetFolderProperty(HIVEHISTORYFILELOC, "tmp", hiveConf, basedir);

		// HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS is removed from Hive 3.1.0
		hiveConf.setBoolean("hive.warehouse.subdir.inherit.perms", true);

		createAndSetFolderProperty("hadoop.tmp.dir", "hadooptmp", hiveConf, basedir);
		createAndSetFolderProperty("test.log.dir", "logs", hiveConf, basedir);
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
