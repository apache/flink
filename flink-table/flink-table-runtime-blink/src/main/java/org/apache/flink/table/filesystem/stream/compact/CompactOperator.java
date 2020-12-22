/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CompactionUnit;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.CoordinatorOutput;
import org.apache.flink.table.filesystem.stream.compact.CompactMessages.EndCompaction;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Receives compaction units to do compaction. Send partition commit information after
 * compaction finished.
 *
 * <p>Use {@link BulkFormat} to read and use {@link BucketWriter} to write.
 *
 * <p>STATE: This operator stores expired files in state, after the checkpoint completes successfully,
 *           We can ensure that these files will not be used again and they can be deleted from the
 *           file system.
 */
public class CompactOperator<T> extends AbstractStreamOperator<PartitionCommitInfo>
		implements OneInputStreamOperator<CoordinatorOutput, PartitionCommitInfo>, BoundedOneInput {

	private static final long serialVersionUID = 1L;

	public static final String UNCOMPACTED_PREFIX = ".uncompacted-";

	public static final String COMPACTED_PREFIX = "compacted-";

	private final SupplierWithException<FileSystem, IOException> fsFactory;
	private final CompactReader.Factory<T> readerFactory;
	private final CompactWriter.Factory<T> writerFactory;

	private transient FileSystem fileSystem;

	private transient ListState<Map<Long, List<Path>>> expiredFilesState;
	private transient TreeMap<Long, List<Path>> expiredFiles;
	private transient List<Path> currentExpiredFiles;

	private transient Set<String> partitions;

	public CompactOperator(
			SupplierWithException<FileSystem, IOException> fsFactory,
			CompactReader.Factory<T> readerFactory,
			CompactWriter.Factory<T> writerFactory) {
		this.fsFactory = fsFactory;
		this.readerFactory = readerFactory;
		this.writerFactory = writerFactory;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		this.partitions = new HashSet<>();
		this.fileSystem = fsFactory.get();

		ListStateDescriptor<Map<Long, List<Path>>> metaDescriptor =
				new ListStateDescriptor<>("expired-files", new MapSerializer<>(
						LongSerializer.INSTANCE,
						new ListSerializer<>(new KryoSerializer<>(Path.class, getExecutionConfig()))
				));
		this.expiredFilesState = context.getOperatorStateStore().getListState(metaDescriptor);
		this.expiredFiles = new TreeMap<>();
		this.currentExpiredFiles = new ArrayList<>();

		if (context.isRestored()) {
			this.expiredFiles.putAll(this.expiredFilesState.get().iterator().next());
		}
	}

	@Override
	public void processElement(StreamRecord<CoordinatorOutput> element) throws Exception {
		CoordinatorOutput value = element.getValue();
		if (value instanceof CompactionUnit) {
			CompactionUnit unit = (CompactionUnit) value;
			if (unit.isTaskMessage(
					getRuntimeContext().getNumberOfParallelSubtasks(),
					getRuntimeContext().getIndexOfThisSubtask())) {
				String partition = unit.getPartition();
				List<Path> paths = unit.getPaths();

				doCompact(partition, paths);
				this.partitions.add(partition);

				// Only after the current checkpoint is successfully executed can delete
				// the expired files, so as to ensure the existence of the files.
				this.currentExpiredFiles.addAll(paths);
			}
		} else if (value instanceof EndCompaction) {
			endCompaction(((EndCompaction) value).getCheckpointId());
		}
	}

	private void endCompaction(long checkpoint) {
		this.output.collect(new StreamRecord<>(new PartitionCommitInfo(
				checkpoint,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks(),
				new ArrayList<>(this.partitions))));
		this.partitions.clear();
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		snapshotState(context.getCheckpointId());
	}

	private void snapshotState(long checkpointId) throws Exception {
		expiredFilesState.clear();
		expiredFiles.put(checkpointId, new ArrayList<>(currentExpiredFiles));
		expiredFilesState.add(expiredFiles);
		currentExpiredFiles.clear();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		clearExpiredFiles(checkpointId);
	}

	@Override
	public void endInput() throws Exception {
		endCompaction(Long.MAX_VALUE);
		snapshotState(Long.MAX_VALUE);
		clearExpiredFiles(Long.MAX_VALUE);
	}

	private void clearExpiredFiles(long checkpointId) throws IOException {
		// Don't need these metas anymore.
		NavigableMap<Long, List<Path>> outOfDateMetas = expiredFiles.headMap(checkpointId, true);
		for (List<Path> paths : outOfDateMetas.values()) {
			for (Path meta : paths) {
				fileSystem.delete(meta, true);
			}
		}
		outOfDateMetas.clear();
	}

	/**
	 * Do Compaction:
	 * - Target file exists, do nothing.
	 * - Can do compaction:
	 *      - Single file, do atomic renaming, there are optimizations for FileSystem.
	 *      - Multiple file, do reading and writing.
	 */
	private void doCompact(String partition, List<Path> paths) throws IOException {
		if (paths.size() == 0) {
			return;
		}

		Path target = createCompactedFile(paths);
		if (fileSystem.exists(target)) {
			return;
		}

		checkExist(paths);

		long startMillis = System.currentTimeMillis();

		boolean success = false;
		if (paths.size() == 1) {
			// optimizer for single file
			success = doSingleFileMove(paths.get(0), target);
		}

		if (!success) {
			doMultiFilesCompact(partition, paths, target);
		}

		double costSeconds = ((double) (System.currentTimeMillis() - startMillis)) / 1000;
		LOG.info("Compaction time cost is '{}S', target file is '{}', input files are '{}'",
				costSeconds, target, paths);
	}

	private boolean doSingleFileMove(Path src, Path dst) throws IOException {
		// We can not rename, because we need to keep original file for failover
		RecoverableWriter writer;
		try {
			writer = fileSystem.createRecoverableWriter();
		} catch (UnsupportedOperationException ignore) {
			// Some writer not support RecoverableWriter, so fallback to per record moving.
			// For example, see the constructor of HadoopRecoverableWriter. Although it not support
			// RecoverableWriter, but HadoopPathBasedBulkFormatBuilder can support streaming writing.
			return false;
		}

		RecoverableFsDataOutputStream out = writer.open(dst);
		try (FSDataInputStream in = fileSystem.open(src)) {
			IOUtils.copyBytes(in, out, false);
		} catch (Throwable t) {
			out.close();
			throw t;
		}
		out.closeForCommit().commit();
		return true;
	}

	private void doMultiFilesCompact(String partition, List<Path> files, Path dst) throws IOException {
		Configuration config = getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
		CompactWriter<T> writer = writerFactory.create(
				CompactContext.create(config, fileSystem, partition, dst));

		for (Path path : files) {
			try (CompactReader<T> reader = readerFactory.create(
					CompactContext.create(config, fileSystem, partition, path))) {
				T record;
				while ((record = reader.read()) != null) {
					writer.write(record);
				}
			}
		}

		// commit immediately
		writer.commit();
	}

	private void checkExist(List<Path> candidates) throws IOException {
		for (Path path : candidates) {
			if (!fileSystem.exists(path)) {
				throw new IOException("Compaction file not exist: " + path);
			}
		}
	}

	private static Path createCompactedFile(List<Path> uncompactedFiles) {
		Path path = convertFromUncompacted(uncompactedFiles.get(0));
		return new Path(path.getParent(), COMPACTED_PREFIX + path.getName());
	}

	public static String convertToUncompacted(String path) {
		return UNCOMPACTED_PREFIX + path;
	}

	public static Path convertFromUncompacted(Path path) {
		Preconditions.checkArgument(
				path.getName().startsWith(UNCOMPACTED_PREFIX),
				"This should be uncompacted file: " + path);
		return new Path(path.getParent(), path.getName().substring(UNCOMPACTED_PREFIX.length()));
	}
}
