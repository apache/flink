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
package org.apache.flink.streaming.connectors.pulsar.internal;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils.IncompatibleSchemaException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Helper class that responsible for catalog administration.
 */
public class PulsarMetadataReader implements AutoCloseable {

	protected static final Logger LOG = LoggerFactory.getLogger(PulsarMetadataReader.class);

	private final String adminUrl;

	private final String driverGroupIdPrefix;

	private final Map<String, String> caseInsensitiveParams;

	private final int indexOfThisSubtask;

	private final int numParallelSubtasks;

	private final PulsarAdmin admin;

	private volatile boolean closed = false;

	public PulsarMetadataReader(
		String adminUrl,
		String driverGroupIdPrefix,
		Map<String, String> caseInsensitiveParams,
		int indexOfThisSubtask,
		int numParallelSubtasks) throws PulsarClientException {

		this.adminUrl = adminUrl;
		this.driverGroupIdPrefix = driverGroupIdPrefix;
		this.caseInsensitiveParams = caseInsensitiveParams;
		this.indexOfThisSubtask = indexOfThisSubtask;
		this.numParallelSubtasks = numParallelSubtasks;
		this.admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
	}

	@Override
	public void close() {
		closed = true;
		admin.close();
	}

	public List<String> listNamespaces() throws PulsarAdminException {
		List<String> tenants = admin.tenants().getTenants();
		List<String> namespaces = new ArrayList<String>();
		for (String tenant : tenants) {
			namespaces.addAll(admin.namespaces().getNamespaces(tenant));
		}
		return namespaces;
	}

	public boolean namespaceExists(String ns) throws PulsarAdminException {
		try {
			admin.namespaces().getTopics(ns);
		} catch (PulsarAdminException.NotFoundException e) {
			return false;
		}
		return true;
	}

	public void createNamespace(String ns) throws PulsarAdminException {
		String nsName = NamespaceName.get(ns).toString();
		admin.namespaces().createNamespace(nsName);
	}

	public void deleteNamespace(String ns) throws PulsarAdminException {
		String nsName = NamespaceName.get(ns).toString();
		admin.namespaces().deleteNamespace(nsName);
	}

	public List<String> getTopics(String ns) throws PulsarAdminException {
		List<String> nonPartitionedTopics = getNonPartitionedTopics(ns);
		List<String> partitionedTopics = admin.topics().getPartitionedTopicList(ns);
		List<String> allTopics = new ArrayList<>();
		Stream.of(partitionedTopics, nonPartitionedTopics).forEach(allTopics::addAll);
		return allTopics.stream().map(t -> TopicName.get(t).getLocalName()).collect(Collectors.toList());
	}

	public TableSchema getTableSchema(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		FieldsDataType fieldsDataType = null;
		try {
			fieldsDataType = getSchema(Collections.singletonList(topicName));
		} catch (IncompatibleSchemaException e) {
			throw new PulsarAdminException(e);
		}
		return SchemaUtils.toTableSchema(fieldsDataType);
	}

	public boolean topicExists(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
		if (partitionNum > 0) {
			return true;
		} else {
			admin.topics().getStats(topicName);
		}
		return true;
	}

	public void deleteTopic(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
		if (partitionNum > 0) {
			admin.topics().deletePartitionedTopic(topicName, true);
		} else {
			admin.topics().delete(topicName, true);
		}
	}

	public void createTopic(ObjectPath objectPath, int defaultPartitionNum, CatalogBaseTable table) throws PulsarAdminException, IncompatibleSchemaException {
		String topicName = objectPath2TopicName(objectPath);
		admin.topics().createPartitionedTopic(topicName, defaultPartitionNum);
	}

	public void putSchema(ObjectPath tablePath, CatalogBaseTable table) throws IncompatibleSchemaException {
		String topic = objectPath2TopicName(tablePath);
		TableSchema tableSchema = table.getSchema();
		List<String> fieldsRemaining = new ArrayList<>(tableSchema.getFieldCount());
		for (String fieldName : tableSchema.getFieldNames()) {
			if (!PulsarOptions.META_FIELD_NAMES.contains(fieldName)) {
				fieldsRemaining.add(fieldName);
			}
		}

		DataType dataType;

		if (fieldsRemaining.size() == 1) {
			dataType = tableSchema.getFieldDataType(fieldsRemaining.get(0)).get();
		} else {
			List<DataTypes.Field> fieldList = fieldsRemaining.stream()
				.map(f -> DataTypes.FIELD(f, tableSchema.getFieldDataType(f).get()))
				.collect(Collectors.toList());
			dataType = DataTypes.ROW(fieldList.toArray(new DataTypes.Field[0]));
		}

		SchemaInfo si = SchemaUtils.sqlType2PulsarSchema(dataType).getSchemaInfo();
		SchemaUtils.uploadPulsarSchema(admin, topic, si);
	}

	public FieldsDataType getSchema(List<String> topics) throws IncompatibleSchemaException {
		SchemaInfo si = getPulsarSchema(topics);
		return SchemaUtils.pulsarSourceSchema(si);
	}

	public SchemaInfo getPulsarSchema(List<String> topics) throws IncompatibleSchemaException {
		Set<SchemaInfo> schemas = new HashSet<>();
		if (topics.size() > 0) {
			topics.forEach(t -> schemas.add(getPulsarSchema(t)));

			if (schemas.size() != 1) {
				throw new IncompatibleSchemaException(
					String.format("Topics to read must share identical schema, however we got %d distinct schemas [%s]",
						schemas.size(),
						String.join(",", schemas.stream().map(SchemaInfo::toString).collect(Collectors.toList()))),
					null);
			}
			return Iterables.getFirst(schemas, SchemaUtils.emptySchemaInfo());
		} else {
			return SchemaUtils.emptySchemaInfo();
		}
	}

	public SchemaInfo getPulsarSchema(String topic) {
		try {
			return admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
		} catch (Throwable e) {
			if (e instanceof PulsarAdminException && ((PulsarAdminException) e).getStatusCode() == 404) {
				return BytesSchema.of().getSchemaInfo();
			} else {
				throw new RuntimeException(
					String.format("Failed to get schema information for %s", TopicName.get(topic).toString()), e);
			}
		}
	}

	public Set<String> getTopicPartitionsAll() throws PulsarAdminException {
		List<String> topics = getTopics();
		HashSet<String> allTopics = new HashSet<>();
		for (String topic : topics) {
			int partNum = admin.topics().getPartitionedTopicMetadata(topic).partitions;
			if (partNum == 0) {
				allTopics.add(topic);
			} else {
				for (int i = 0; i < partNum; i++) {
					allTopics.add(topic + PulsarOptions.PARTITION_SUFFIX + i);
				}
			}
		}
		return allTopics;
	}

	public List<String> getTopics() throws PulsarAdminException {
		for (Map.Entry<String, String> e : caseInsensitiveParams.entrySet()) {
			if (PulsarOptions.TOPIC_OPTION_KEYS.contains(e.getKey())) {
				String key = e.getKey();
				if (key.equals("topic")) {
					return Collections.singletonList(TopicName.get(e.getValue()).toString());
				} else if (key.equals("topics")) {
					return Arrays.asList(e.getValue().split(",")).stream()
						.filter(s -> !s.isEmpty())
						.map(t -> TopicName.get(t).toString())
						.collect(Collectors.toList());
				} else { // topicspattern
					return getTopicsWithPattern(e.getValue());
				}
			}
		}
		return null;
	}

	private List<String> getTopicsWithPattern(String topicsPattern) throws PulsarAdminException {
		TopicName dest = TopicName.get(topicsPattern);
		List<String> allNonPartitionedTopics = getNonPartitionedTopics(dest.getNamespace());
		List<String> nonPartitionedMatch = topicsPatternFilter(allNonPartitionedTopics, dest.toString());

		List<String> allPartitionedTopics = admin.topics().getPartitionedTopicList(dest.getNamespace());
		List<String> partitionedMatch = topicsPatternFilter(allPartitionedTopics, dest.toString());

		return ListUtils.union(nonPartitionedMatch, partitionedMatch);
	}

	private List<String> getNonPartitionedTopics(String namespace) throws PulsarAdminException {
		return admin.topics().getList(namespace).stream()
			.filter(t -> !TopicName.get(t).isPartitioned())
			.collect(Collectors.toList());
	}

	private List<String> topicsPatternFilter(List<String> allTopics, String topicsPattern) {
		Pattern shortenedTopicsPattern = Pattern.compile(topicsPattern.split("\\:\\/\\/")[1]);
		return allTopics.stream().map(t -> TopicName.get(t).toString())
			.filter(t -> shortenedTopicsPattern.matcher(t.split("\\:\\/\\/")[1]).matches())
			.collect(Collectors.toList());
	}

	public static String objectPath2TopicName(ObjectPath objectPath) {
		NamespaceName ns = NamespaceName.get(objectPath.getDatabaseName());
		String topic = objectPath.getObjectName();
		TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
		return fullName.toString();
	}

	public static class ClosedException extends Exception {

	}
}
