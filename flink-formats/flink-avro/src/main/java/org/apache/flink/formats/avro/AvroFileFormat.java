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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.avro.utils.AvroUtils;
import org.apache.flink.formats.avro.utils.FSDataInputStreamWrapper;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A File Format for reading Avro files via the unified batch/streaming file source
 * ({@link org.apache.flink.connector.file.src.FileSource}).
 *
 * @param <E> The type of the records read by this reader.
 */
public class AvroFileFormat<E> implements StreamFormat<E> {

	private static final long serialVersionUID = 1L;

	static final Logger LOG = LoggerFactory.getLogger(AvroFileFormat.class);

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	/**
	 * Creates a new Avro File Format that reads Avro {@link org.apache.avro.specific.SpecificData specific records}
	 * or records via Avro's {@link org.apache.avro.reflect.ReflectData reflection reader}.
	 *
	 * <p>To read into Avro {@link GenericRecord} types, use the {@link #forGenericType(Schema)} method.
	 *
	 * @see #forGenericType(Schema)
	 */
	public static <T> AvroFileFormat<T> forSpecificOrReflectType(final Class<T> typeClass) {
		final TypeInformation<T> typeInfo;
		if (SpecificRecordBase.class.isAssignableFrom(typeClass)) {
			final TypeInformation<? extends SpecificRecordBase> specTypeInfo =
					new AvroTypeInfo<>(typeClass.asSubclass(SpecificRecordBase.class));

			// juggle a bit with the generic casting
			@SuppressWarnings("unchecked")
			final TypeInformation<T> castedInfo = (TypeInformation<T>) specTypeInfo;
			typeInfo = castedInfo;
		} else if (GenericRecord.class.isAssignableFrom(typeClass)) {
			throw new IllegalArgumentException(
					"Cannot read and create Avro GenericRecord without specifying the Avro Schema. " +
					"That is because Flink needs to be able serialize the results in its data flow, which is" +
					"very inefficient without the schema. And while the Schema is stored in the Avro file header," +
					"Flink needs this schema during 'pre-flight' time when the data flow is set up and wired," +
					"which is before there is access to the files");
		} else {
			// this is a PoJo that Avo will reader via reflect de-serialization
			// for Flink, this is just a plain PoJo type
			typeInfo = TypeExtractor.createTypeInfo(typeClass);
		}

		return new AvroFileFormat<>(typeInfo);
	}

	/**
	 * Creates a new Avro File Format that reads the file into Avro {@link GenericRecord}s.
	 *
	 * <p>To read into {@code GenericRecords}, this method needs an Avro Schema.
	 * That is because Flink needs to be able serialize the results in its data flow, which is
	 * very inefficient without the schema. And while the Schema is stored in the Avro file header,
	 * Flink needs this schema during 'pre-flight' time when the data flow is set up and wired," +
	 * which is before there is access to the files.
	 */
	public static AvroFileFormat<GenericRecord> forGenericType(final Schema schema) {
		final GenericRecordAvroTypeInfo type = new GenericRecordAvroTypeInfo(schema);
		return new AvroFileFormat<>(type);
	}

	// ------------------------------------------------------------------------
	//  Stream File Format
	// ------------------------------------------------------------------------

	private final TypeInformation<E> type;

	private AvroFileFormat(TypeInformation<E> type) {
		this.type = type;
	}

	@Override
	public TypeInformation<E> getProducedType() {
		return type;
	}

	@Override
	public boolean isSplittable() {
		return true;
	}

	@Override
	public Reader<E> createReader(
			final Configuration config,
			final FSDataInputStream stream,
			final long fileLen,
			final long splitEnd) throws IOException {

		// creating the reader will sync to 0, so we need to remember the position and sync back
		// to this position at the end.
		final long startPos = stream.getPos();

		final DatumReader<E> datumReader = AvroUtils.createDatumReader(type);
		final SeekableInput in = new FSDataInputStreamWrapper(stream, fileLen);
		final DataFileReader<E> avroDataFileReader = (DataFileReader<E>) DataFileReader.openReader(in, datumReader);

		LOG.debug("Loaded Avro Schema: {}", avroDataFileReader.getSchema());

		avroDataFileReader.sync(startPos);
		final long lastSync = avroDataFileReader.previousSync();

		return new AvroReader<>(avroDataFileReader, splitEnd, lastSync);
	}

	@Override
	public Reader<E> restoreReader(
			final Configuration config,
			final FSDataInputStream stream,
			final long restoredOffset,
			final long fileLen,
			final long splitEnd) throws IOException {

		// Restoring a reader is here in fact identical to creating a reader, from the appropriate
		// position in the stream
		// Avro advances to the next sync marker from the offset, and if the offset happened
		// to be an exact sync marker (like when the position comes from a checkpoint), the position
		// will remain unchanged
		stream.seek(restoredOffset);
		return createReader(config, stream, fileLen, splitEnd);
	}

	// ------------------------------------------------------------------------
	//  reader implementation
	// ------------------------------------------------------------------------

	/**
	 * A reader for file sources that reads records via an Avro {@link DataFileReader}.
	 *
	 * @param <E> The type of the records read by this reader.
	 */
	public static class AvroReader<E> implements StreamFormat.Reader<E> {

		private final DataFileReader<E> avroDataFileReader;
		private final long splitEnd;
		private long checkpointSyncOffset;
		private long numRecordsSinceSyncOffset;

		public AvroReader(DataFileReader<E> avroDataFileReader, long splitEnd, long checkpointSyncOffset) {
			this.avroDataFileReader = avroDataFileReader;
			this.splitEnd = splitEnd;
			this.checkpointSyncOffset = checkpointSyncOffset;
			this.numRecordsSinceSyncOffset = 0;
		}

		@Nullable
		@Override
		public E read() throws IOException {
			if (avroDataFileReader.pastSync(splitEnd) || !avroDataFileReader.hasNext()) {
				return null;
			}

			final E next = avroDataFileReader.next();

			// When the call to 'getCheckpointedPosition()' comes, we need to give the position
			// where the reader is then, i.e., after the current record. That's why we only build
			// the position information after we read the record.
			final long currentSync = avroDataFileReader.previousSync();
			if (currentSync != checkpointSyncOffset) {
				checkpointSyncOffset = currentSync;
				numRecordsSinceSyncOffset = 0;
			} else {
				numRecordsSinceSyncOffset++;
			}

			return next;
		}

		@Override
		public void close() throws IOException {
			avroDataFileReader.close();
		}

		@Override
		public CheckpointedPosition getCheckpointedPosition() {
			return new CheckpointedPosition(checkpointSyncOffset, numRecordsSinceSyncOffset);
		}
	}
}
