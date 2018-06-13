package org.apache.flink.table.client.gateway.local;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class EnvironmentMerging {

	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";
	private static final String TABLE_SOURCE_FACTORY_JAR_FILE = "table-source-factory-test-jar.jar";
	private static final String[] TABLE_NAMES = {"MergedTable", "TableName1"};
	@Test
	public void testIfTwoFilesAreMergedProperly() throws IOException {
		Environment env1 = EnvironmentFileUtil.parseUnmodified("test-sql-client-defaults.yaml");
		Environment env2 = EnvironmentFileUtil.parseModified("test-sql-client-factory.yaml", Collections.singletonMap("TableName1", "MergedTable"));

		Set<String> mergedTableNames = Sets.union(env1.getTables().keySet(), env2.getTables().keySet());

		Map<String, String> mergedExecutionProperties = new HashMap<>();
		mergedExecutionProperties.putAll(env1.getExecution().toProperties());
		mergedExecutionProperties.putAll(env2.getExecution().toProperties());

		Map<String, String> mergedDeploymentProperties = new HashMap<>();
		mergedDeploymentProperties.putAll(env1.getDeployment().toProperties());
		mergedDeploymentProperties.putAll(env2.getDeployment().toProperties());

		Environment merged = Environment.merge(env1, env2);

		Assert.assertEquals(mergedTableNames.size(), merged.getTables().size());
		Assert.assertEquals(mergedTableNames, merged.getTables().keySet());
		Assert.assertEquals(mergedDeploymentProperties, merged.getDeployment().toProperties());
		Assert.assertEquals(mergedExecutionProperties, merged.getExecution().toProperties());
	}
}
