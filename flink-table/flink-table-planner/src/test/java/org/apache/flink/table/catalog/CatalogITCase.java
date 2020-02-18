package org.apache.flink.table.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;
import static org.junit.Assert.assertTrue;

/**
 * IT Case for catalog ddl.
 */
public class CatalogITCase extends AbstractTestBase {

	@Test
	public void testCreateCatalog() {
		String name = "c1";
		TableEnvironment tableEnv = getTableEnvironment();
		String ddl = String.format("create catalog %s with('type'='%s')", name, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);

		tableEnv.sqlUpdate(ddl);

		assertTrue(tableEnv.getCatalog(name).isPresent());
		assertTrue(tableEnv.getCatalog(name).get() instanceof GenericInMemoryCatalog);
	}

	private TableEnvironment getTableEnvironment() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return StreamTableEnvironment.create(env, settings);
	}
}
