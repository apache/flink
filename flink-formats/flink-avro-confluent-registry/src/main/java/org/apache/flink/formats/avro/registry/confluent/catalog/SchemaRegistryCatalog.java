/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent.catalog;

import org.apache.flink.formats.avro.registry.confluent.RegistryAvroOptions;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Catalog for
 * <a href="https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html">Confluent Schema Registry</a>.
 * It allows to access all the topics of current Confluent Schema Registry Service
 * through SQL or TableAPI, there is no need to create any table explicitly.
 *
 * <p>The code snippet below illustrates how to use this catalog:
 * <pre>
 *      String schemaRegistryURL = ...;
 * 		Map&lt;String, String&gt; kafkaProps = ...;
 * 		SchemaRegistryCatalog catalog = SchemaRegistryCatalog.builder()
 * 				.schemaRegistryURL(schemaRegistryURL)
 * 				.kafkaOptions(kafkaProps)
 * 				.catalogName("myCatalog")
 * 				.dbName("myDB")
 * 				.build();
 * 		tEnv.registerCatalog("catalog1", catalog);
 *
 * 		// ---------- Consume stream from Kafka -------------------
 *
 * 		// Assumes there is a topic named 'transactions'
 * 		String query = "SELECT\n" +
 * 			"  id, amount\n" +
 * 			"FROM myCatalog.myDB.transactions";
 * </pre>
 *
 * <p>We only support TopicNameStrategy for subject naming strategy,
 * for which all the records in one topic has the same schema, see
 * <a href="https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work">How the Naming Strategies Work</a>
 * for details.
 *
 * <p>You can specify some common options for these topics. All the tables from this catalog
 * would take the same options. If this is not your request, use dynamic table options setting up
 * within per-table scope.
 *
 * <p>The limitations:
 * <ul>
 *     <li>The catalog only supports reading messages with the latest enabled schema for any given
 *     Kafka topic at the time when the SQL query was compiled.</li>
 *     <li>No time-column and watermark support.</li>
 *     <li>The catalog is read-only. It does not support table creations
 *     or deletions or modifications.</li>
 *     <li>The catalog only supports Kafka message values prefixed with schema id,
 *     this is also the default behavior for the SchemaRegistry Kafka producer format.</li>
 * </ul>
 */
public class SchemaRegistryCatalog extends TableCatalog {
	private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	private static final String CONNECTOR_PROPS_ERROR_FORMAT =
			"No need to specify property '%s' for "
					+ SchemaRegistryCatalog.class.getSimpleName();

	/** URL to connect to the Schema Registry Service. */
	private String schemaRegistryURL;

	/** Client to interact with the Confluent Schema Registry. */
	private SchemaRegistryClient client;

	/** All topic name list snapshot of the schema registry when this catalog is open. */
	private List<String> allTopics = Collections.emptyList();

	// Singleton database for this catalog with default name.
	private CatalogDatabase database;

	/** Additional connector options for all the Kafka topics. */
	private Map<String, String> connectorOptions;

	private Cache<String, CatalogBaseTable> tableCache;

	private SchemaRegistryCatalog(
			Map<String, String> connectorOptions,
			String schemaRegistryURL,
			SchemaRegistryClient client,
			String defaultDatabase) {
		// The catalog name is actually useless.
		super("kafka", defaultDatabase);

		this.schemaRegistryURL = schemaRegistryURL;

		this.connectorOptions = connectorOptions;

		this.client = client;

		this.tableCache = CacheBuilder.newBuilder()
				.maximumSize(DEFAULT_IDENTITY_MAP_CAPACITY)
				.softValues()
				.build();
	}

	/**
	 * Returns a builder to construct the catalog.
	 */
	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void open() throws CatalogException {
		this.database = new CatalogDatabaseImpl(Collections.emptyMap(),
				String.format("SchemaRegistryCatalog database: %s", getDefaultDatabase()));
		try {
			this.allTopics = client.getAllSubjects().stream()
					.filter(sub -> sub.endsWith("-value"))
					// Strips out the "-value" suffix.
					.map(sub -> sub.substring(0, sub.length() - 6))
					.collect(Collectors.toList());
		} catch (IOException | RestClientException e) {
			throw new CatalogException("Error when fetching topic name list", e);
		}
	}

	@Override
	public void close() throws CatalogException {
		this.allTopics.clear();
		this.tableCache.invalidateAll();
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		return Collections.singletonList(getDefaultDatabase());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Preconditions.checkNotNull(databaseName);
		if (!databaseName.equals(getDefaultDatabase())) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		return database;
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return Objects.equals(databaseName, getDefaultDatabase());
	}

	@Override
	public List<String> listTables(String databaseName) throws CatalogException {
		return this.allTopics;
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		} else {
			final String tableName = tablePath.getObjectName();
			try {
				return this.tableCache.get(tableName, () -> generatesCatalogTable(tableName));
			} catch (ExecutionException e) {
				throw new RuntimeException("Error while get table from the cache.");
			}
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		String dbName = tablePath.getDatabaseName();
		return databaseExists(dbName) && allTopics.contains(tablePath.getObjectName());
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws CatalogException {
		// Can be supported in the near future.
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	//~ Tools ------------------------------------------------------------------

	private CatalogBaseTable generatesCatalogTable(String topicName) throws RestClientException {
		try {
			final String subject = String.format("%s-value", topicName);
			SchemaMetadata metadata = this.client.getLatestSchemaMetadata(subject);
			DataType dataType = AvroSchemaConverter.convertToDataType(metadata.getSchema());
			Preconditions.checkState(dataType.getLogicalType() instanceof RowType,
					String.format("The schema string: %s for subject %s can not be parsed as a row type",
							metadata.getSchema(),
							subject));
			RowType rowType = (RowType) dataType.getLogicalType();
			TableSchema tableSchema = TableSchema.builder()
					.fields(
							rowType.getFieldNames().toArray(new String[0]),
							dataType.getChildren().toArray(new DataType[0]))
					.build();
			Map<String, String> connectorProps = new HashMap<>(this.connectorOptions);
			final String format = metadata.getSchemaType().toLowerCase() + "-confluent";
			// Reference KafkaOptions for the keys.
			connectorProps.put("topic", topicName);
			connectorProps.put(FactoryUtil.FORMAT.key(), format);
			connectorProps.put(
					format + "." + RegistryAvroOptions.SCHEMA_REGISTRY_URL.key(),
					this.schemaRegistryURL);
			connectorProps.put(
					format + "." + RegistryAvroOptions.SCHEMA_REGISTRY_SUBJECT.key(),
					subject);

			return new CatalogTableImpl(
					tableSchema,
					connectorProps,
					"Schema Registry table for topic: " + topicName);
		} catch (IOException e) {
			throw new CatalogException("Error while fetching schema info "
					+ "from Confluent Schema Registry", e);
		}
	}

	//~ Inner Class ------------------------------------------------------------

	/** Builder for {@link SchemaRegistryCatalog}. */
	public static class Builder {
		private Map<String, String> kafkaOptions;
		private String schemaRegistryURL;
		private String defaultDatabase;

		@Nullable
		private SchemaRegistryClient client;

		private Builder () {
			this.defaultDatabase = "kafka";
			this.kafkaOptions = Collections.emptyMap();
		}

		/**
		 * Sets up the Kafka connector table options for all the tables read from the catalog,
		 * the options are shared for all the tables, if you want to tweak or override per-table scope,
		 * use the dynamic table options or CREATE TABLE LIKE syntax.
		 */
		public Builder kafkaOptions(Map<String, String> props) {
			Objects.requireNonNull(props);
			validateForbiddenOptions(props);

			Map<String, String> overriddenProps = new HashMap<>(props);
			overriddenProps.put(FactoryUtil.CONNECTOR.key(), "kafka");
			this.kafkaOptions = Collections.unmodifiableMap(overriddenProps);
			return this;
		}

		/**
		 * Sets up the Schema Registry URL to connect to the registry service.
		 */
		public Builder schemaRegistryURL(String schemaRegistryURL) {
			Objects.requireNonNull(schemaRegistryURL);
			this.schemaRegistryURL = schemaRegistryURL;
			return this;
		}

		/** Sets up the database name, default is 'kafka'. */
		public Builder dbName(String dbName) {
			this.defaultDatabase = Objects.requireNonNull(dbName);
			return this;
		}

		/**
		 * Sets up the {@link SchemaRegistryClient} to connect to the registry service.
		 * By default, the catalog holds a {@link CachedSchemaRegistryClient} with 1000
		 * as {@code identityMapCapacity}.
		 *
		 * <p>This method is used for custom client configuration, i.e. the SSL configurations
		 * or to change the default {@code identityMapCapacity}.
		 */
		public Builder registryClient(SchemaRegistryClient client) {
			this.client = Objects.requireNonNull(client);
			return this;
		}

		public SchemaRegistryCatalog build() {
			Preconditions.checkState(schemaRegistryURL != null,
					"Schema Registry URL should be set through Builder.schemaRegistryURL.");
			// Validate required keys for Kafka.
			String serverKey = "properties.bootstrap.servers";
			boolean missingKeys = this.kafkaOptions.keySet().stream()
					.noneMatch(k -> k.equalsIgnoreCase(serverKey));
			if (missingKeys) {
				throw new ValidationException(String.format("Option %s is required for Kafka, " +
						"please configure through Builder.kafkaOptions.", serverKey));
			}
			return new SchemaRegistryCatalog(
					this.kafkaOptions,
					this.schemaRegistryURL,
					this.client == null
							? new CachedSchemaRegistryClient(schemaRegistryURL, DEFAULT_IDENTITY_MAP_CAPACITY)
							: this.client,
					this.defaultDatabase);
		}

		private static void validateForbiddenOptions(Map<String, String> connectorProps) {
			// validate the connector properties are valid.
			DescriptorProperties descriptorProperties = new DescriptorProperties();
			descriptorProperties.putProperties(Objects.requireNonNull(connectorProps));
			Set<String> forbiddenConnectorProps = new HashSet<>(2);
			forbiddenConnectorProps.add(FactoryUtil.CONNECTOR.key());
			forbiddenConnectorProps.add(FactoryUtil.FORMAT.key());
			for (String forbidden : forbiddenConnectorProps) {
				if (descriptorProperties.containsKey(forbidden)) {
					throw new ValidationException(String.format(CONNECTOR_PROPS_ERROR_FORMAT,
							forbidden));
				}
			}
		}
	}
}
