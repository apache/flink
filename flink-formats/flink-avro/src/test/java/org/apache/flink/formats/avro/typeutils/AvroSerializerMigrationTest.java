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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertSame;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests migrations for {@link AvroSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class AvroSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	private static final String DATA = "flink-1.6-avro-type-serializer-address-data";
	private static final String SPECIFIC_SNAPSHOT = "flink-1.6-avro-type-serializer-address-snapshot";
	private static final String GENERIC_SNAPSHOT = "flink-1.6-avro-generic-type-serializer-address-snapshot";

	public AvroSerializerMigrationTest(TestSpecification<Object> testSpec) {
		super(testSpec);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object[]> testSpecifications() {

		final TestSpecification<Address> genericCase = TestSpecification.<Address>builder("1.6-generic", AvroSerializer.class, AvroSerializerSnapshot.class)
			.withSerializerProvider(() -> new AvroSerializer(GenericRecord.class, Address.getClassSchema()))
			.withSnapshotDataLocation(GENERIC_SNAPSHOT)
			.withTestData(DATA, 10);

		final TestSpecification<Address> specificCase = TestSpecification.<Address>builder("1.6-specific", AvroSerializer.class, AvroSerializerSnapshot.class)
			.withSerializerProvider(() -> new AvroSerializer<>(Address.class))
			.withSnapshotDataLocation(SPECIFIC_SNAPSHOT)
			.withTestData(DATA, 10);

		return Arrays.asList(
			new Object[]{genericCase},
			new Object[]{specificCase}
		);
	}

	// ---------------------------------------------------------------------------------------------------------------
	// The following batch of tests are making sure that AvroSerializer class is able to be Java-Deserialized.
	// see [FLINK-11436] for more information.

	// Once we drop support for versions that carried snapshots with Java-Deserialized serializers we can drop this
	// batch of tests.
	// ---------------------------------------------------------------------------------------------------------------

	@Test
	public void javaDeserializeFromFlink_1_5_ReflectiveRecord() throws IOException {
		final String avroSerializerBase64 = "AAAAAQAAAQis7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgABTAAEdHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFj\n" +
			"aGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwdnIA\n" +
			"Tm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJvU2VyaWFsaXplck1pZ3Jh\n" +
			"dGlvblRlc3QkU2ltcGxlUG9qbwAAAAAAAAAAAAAAeHA=";

		TypeSerializer<?> serializer = javaDeserialize(avroSerializerBase64);
		assertThat(serializer, instanceOf(AvroSerializer.class));

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) javaDeserialize(avroSerializerBase64);
		assertSame(avroSerializer.getType(), SimplePojo.class);
		assertThat(avroSerializer.getAvroSchema(), notNullValue());
	}

	@Test
	public void javaDeserializeFromFlink_1_5_SpecificRecord() throws IOException {
		final String avroSerializerBase64 = "AAAAAQAAASOs7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgABTAAEdHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFj\n" +
			"aGUuZmxpbmsuYXBpLmNvbW1vbi50eXBldXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwdnIA\n" +
			"L29yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLmdlbmVyYXRlZC5BZGRyZXNz7Paj+KjgQ2oMAAB4\n" +
			"cgArb3JnLmFwYWNoZS5hdnJvLnNwZWNpZmljLlNwZWNpZmljUmVjb3JkQmFzZQKi+azGtzQdDAAAeHA=";

		TypeSerializer<?> serializer = javaDeserialize(avroSerializerBase64);
		assertThat(serializer, instanceOf(AvroSerializer.class));

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) javaDeserialize(avroSerializerBase64);
		assertSame(avroSerializer.getType(), Address.class);
		assertThat(avroSerializer.getAvroSchema(), is(Address.SCHEMA$));
	}

	@Test
	public void javaDeserializeFromFlink_1_6() throws IOException {
		final String avroSerializer = "AAAAAQAAAUis7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgACTAAMc2NoZW1hU3RyaW5ndAASTGphdmEvbGFuZy9TdHJpbmc7TAAE\n" +
			"dHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBl\n" +
			"dXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwcHZyAC9vcmcuYXBhY2hlLmZsaW5rLmZvcm1h\n" +
			"dHMuYXZyby5nZW5lcmF0ZWQuQWRkcmVzc+z2o/io4ENqDAAAeHIAK29yZy5hcGFjaGUuYXZyby5zcGVj\n" +
			"aWZpYy5TcGVjaWZpY1JlY29yZEJhc2UCovmsxrc0HQwAAHhw";

		TypeSerializer<?> avro = javaDeserialize(avroSerializer);

		assertThat(avro, instanceOf(AvroSerializer.class));
	}

	@Test
	public void javaDeserializeFromFlink_1_6_GenericRecord() throws IOException {
		String avroSerializerBase64 = "AAAAAQAAAges7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAABAgACTAAMc2NoZW1hU3RyaW5ndAASTGphdmEvbGFuZy9TdHJpbmc7TAAE\n" +
			"dHlwZXQAEUxqYXZhL2xhbmcvQ2xhc3M7eHIANG9yZy5hcGFjaGUuZmxpbmsuYXBpLmNvbW1vbi50eXBl\n" +
			"dXRpbHMuVHlwZVNlcmlhbGl6ZXIAAAAAAAAAAQIAAHhwdAEBeyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6\n" +
			"IkFkZHJlc3MiLCJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMuYXZyby5nZW5lcmF0\n" +
			"ZWQiLCJmaWVsZHMiOlt7Im5hbWUiOiJudW0iLCJ0eXBlIjoiaW50In0seyJuYW1lIjoic3RyZWV0Iiwi\n" +
			"dHlwZSI6InN0cmluZyJ9LHsibmFtZSI6ImNpdHkiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIjoic3Rh\n" +
			"dGUiLCJ0eXBlIjoic3RyaW5nIn0seyJuYW1lIjoiemlwIiwidHlwZSI6InN0cmluZyJ9XX12cgAlb3Jn\n" +
			"LmFwYWNoZS5hdnJvLmdlbmVyaWMuR2VuZXJpY1JlY29yZAAAAAAAAAAAAAAAeHA=";

		TypeSerializer<?> serializer = javaDeserialize(avroSerializerBase64);

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) serializer;
		assertSame(avroSerializer.getType(), GenericRecord.class);
		assertThat(avroSerializer.getAvroSchema(), notNullValue());
	}

	@Test
	public void javaDeserializeFromFlink_1_7() throws IOException {
		String avroSerializerBase64 = "AAAAAQAAAeKs7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAACAgADTAAOcHJldmlvdXNTY2hlbWF0AEBMb3JnL2FwYWNoZS9mbGluay9m\n" +
			"b3JtYXRzL2F2cm8vdHlwZXV0aWxzL1NlcmlhbGl6YWJsZUF2cm9TY2hlbWE7TAAGc2NoZW1hcQB+AAFM\n" +
			"AAR0eXBldAARTGphdmEvbGFuZy9DbGFzczt4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5\n" +
			"cGV1dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHBzcgA+b3JnLmFwYWNoZS5mbGluay5mb3Jt\n" +
			"YXRzLmF2cm8udHlwZXV0aWxzLlNlcmlhbGl6YWJsZUF2cm9TY2hlbWEAAAAAAAAAAQMAAHhwdwEAeHNx\n" +
			"AH4ABXcBAHh2cgAvb3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkLkFkZHJlc3Ps\n" +
			"9qP4qOBDagwAAHhyACtvcmcuYXBhY2hlLmF2cm8uc3BlY2lmaWMuU3BlY2lmaWNSZWNvcmRCYXNlAqL5\n" +
			"rMa3NB0MAAB4cA==";

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) javaDeserialize(avroSerializerBase64);
		assertSame(avroSerializer.getType(), Address.class);
		assertThat(avroSerializer.getAvroSchema(), is(Address.SCHEMA$));
	}

	@Test
	public void javaDeserializeFromFlink_1_7_afterInitialization() throws IOException {
		String avroSerializerBase64 = "AAAAAQAAAeKs7QAFc3IANm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5hdnJvLnR5cGV1dGlscy5BdnJv\n" +
			"U2VyaWFsaXplcgAAAAAAAAACAgADTAAOcHJldmlvdXNTY2hlbWF0AEBMb3JnL2FwYWNoZS9mbGluay9m\n" +
			"b3JtYXRzL2F2cm8vdHlwZXV0aWxzL1NlcmlhbGl6YWJsZUF2cm9TY2hlbWE7TAAGc2NoZW1hcQB+AAFM\n" +
			"AAR0eXBldAARTGphdmEvbGFuZy9DbGFzczt4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5\n" +
			"cGV1dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHBzcgA+b3JnLmFwYWNoZS5mbGluay5mb3Jt\n" +
			"YXRzLmF2cm8udHlwZXV0aWxzLlNlcmlhbGl6YWJsZUF2cm9TY2hlbWEAAAAAAAAAAQMAAHhwdwEAeHNx\n" +
			"AH4ABXcBAHh2cgAvb3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkLkFkZHJlc3Ps\n" +
			"9qP4qOBDagwAAHhyACtvcmcuYXBhY2hlLmF2cm8uc3BlY2lmaWMuU3BlY2lmaWNSZWNvcmRCYXNlAqL5\n" +
			"rMa3NB0MAAB4cA==";

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) javaDeserialize(avroSerializerBase64);
		assertSame(avroSerializer.getType(), Address.class);
		assertThat(avroSerializer.getAvroSchema(), is(Address.SCHEMA$));
	}

	@Test
	public void compositeSerializerFromFlink_1_6_WithNestedAvroSerializer() throws IOException {
		String streamElementSerializerBase64 = "AAAAAQAAAq2s7QAFc3IAR29yZy5hcGFjaGUuZmxpbmsuc3RyZWFtaW5nLnJ1bnRpbWUuc3RyZWFtcmVj\n" +
			"b3JkLlN0cmVhbUVsZW1lbnRTZXJpYWxpemVyAAAAAAAAAAECAAFMAA50eXBlU2VyaWFsaXplcnQANkxv\n" +
			"cmcvYXBhY2hlL2ZsaW5rL2FwaS9jb21tb24vdHlwZXV0aWxzL1R5cGVTZXJpYWxpemVyO3hyADRvcmcu\n" +
			"YXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZXV0aWxzLlR5cGVTZXJpYWxpemVyAAAAAAAAAAECAAB4\n" +
			"cHNyADZvcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMuYXZyby50eXBldXRpbHMuQXZyb1NlcmlhbGl6ZXIA\n" +
			"AAAAAAAAAQIAAkwADHNjaGVtYVN0cmluZ3QAEkxqYXZhL2xhbmcvU3RyaW5nO0wABHR5cGV0ABFMamF2\n" +
			"YS9sYW5nL0NsYXNzO3hxAH4AAnQBAXsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJBZGRyZXNzIiwibmFt\n" +
			"ZXNwYWNlIjoib3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLmF2cm8uZ2VuZXJhdGVkIiwiZmllbGRzIjpb\n" +
			"eyJuYW1lIjoibnVtIiwidHlwZSI6ImludCJ9LHsibmFtZSI6InN0cmVldCIsInR5cGUiOiJzdHJpbmci\n" +
			"fSx7Im5hbWUiOiJjaXR5IiwidHlwZSI6InN0cmluZyJ9LHsibmFtZSI6InN0YXRlIiwidHlwZSI6InN0\n" +
			"cmluZyJ9LHsibmFtZSI6InppcCIsInR5cGUiOiJzdHJpbmcifV19dnIAJW9yZy5hcGFjaGUuYXZyby5n\n" +
			"ZW5lcmljLkdlbmVyaWNSZWNvcmQAAAAAAAAAAAAAAHhw";

		StreamElementSerializer<?> ser = (StreamElementSerializer<?>) javaDeserialize(streamElementSerializerBase64);
		TypeSerializer<?> containedTypeSerializer = ser.getContainedTypeSerializer();

		assertThat(containedTypeSerializer, instanceOf(AvroSerializer.class));

		AvroSerializer<?> avroSerializer = (AvroSerializer<?>) containedTypeSerializer;
		assertSame(avroSerializer.getType(), GenericRecord.class);
		assertThat(avroSerializer.getAvroSchema(), is(Address.SCHEMA$));
	}

	@Test
	public void makeSureThatFieldsWereNotChanged() {
		// This test should be removed once we completely migrate all the composite serializers.

		List<String> serializedFieldNames = Arrays.stream(AvroSerializer.class.getDeclaredFields())
			.filter(field -> !Modifier.isTransient(field.getModifiers()))
			.filter(field -> !Modifier.isStatic(field.getModifiers()))
			.map(Field::getName)
			.sorted()
			.collect(Collectors.toList());

		assertThat(serializedFieldNames, is(asList("previousSchema", "schema", "type")));
	}

	@SuppressWarnings("deprecation")
	private static TypeSerializer<?> javaDeserialize(String base64) throws IOException {
		byte[] bytes = Base64.getMimeDecoder().decode(base64);
		DataInputDeserializer in = new DataInputDeserializer(bytes);
		return TypeSerializerSerializationUtil.tryReadSerializer(in, Thread.currentThread().getContextClassLoader());
	}

	/**
	 * A simple pojo used in these tests.
	 */
	public static class SimplePojo {
		private String foo;

		@SuppressWarnings("unused")
		public String getFoo() {
			return foo;
		}

		@SuppressWarnings("unused")
		public void setFoo(String foo) {
			this.foo = foo;
		}
	}
}
