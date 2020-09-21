/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitState;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

/**
 * The {@link RecordEmitter} implementation for {@link FileSourceReader}.
 *
 * <p>This updates the {@link FileSourceSplit} for every emitted record.
 * Because the {@link FileSourceSplit} points to the position from where to start reading (after recovery),
 * the current offset and records-to-skip need to always point to the record after the emitted record.
 */
@Internal
final class FileSourceRecordEmitter<T> implements RecordEmitter<RecordAndPosition<T>, T, FileSourceSplitState> {

	@Override
	public void emitRecord(
			final RecordAndPosition<T> element,
			final SourceOutput<T> output,
			final FileSourceSplitState splitState) {

		output.collect(element.getRecord());
		splitState.setPosition(element.getOffset(), element.getRecordSkipCount());
	}
}
