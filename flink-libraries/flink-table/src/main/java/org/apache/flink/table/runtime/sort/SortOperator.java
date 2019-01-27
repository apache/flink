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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.codegen.GeneratedSorter;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.codegen.CodeGenUtils.compile;

/**
 * Operator for sort.
 */
public class SortOperator extends AbstractStreamOperatorWithMetrics<BinaryRow>
		implements OneInputStreamOperator<BaseRow, BinaryRow> {

	private final long reservedMemorySize;
	private final long maxMemorySize;
	private final long perRequestMemorySize;
	private GeneratedSorter gSorter;
	private static final Logger LOG = LoggerFactory.getLogger(SortOperator.class);

	// cooked classes
	protected transient Class<NormalizedKeyComputer> computerClass;
	protected transient Class<RecordComparator> comparatorClass;

	private transient BinaryExternalSorter sorter;
	private transient StreamRecordCollector<BinaryRow> collector;
	private transient BinaryRowSerializer binarySerializer;

	public SortOperator(
			long reservedMemorySize, long maxMemorySize, long perRequestMemorySize,
			GeneratedSorter gSorter) {
		this.reservedMemorySize = reservedMemorySize;
		this.maxMemorySize = maxMemorySize;
		this.perRequestMemorySize = perRequestMemorySize;
		this.gSorter = gSorter;
	}

	@Override
	public void open() throws Exception {
		super.open();
		LOG.info("Opening SortOperator");

		cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());

		TypeSerializer<BaseRow> inputSerializer = getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
		this.binarySerializer =
				new BinaryRowSerializer(((AbstractRowSerializer) inputSerializer).getTypes());

		MemoryManager memManager = this.getContainingTask().getEnvironment().getMemoryManager();
		IOManager ioManager = this.getContainingTask().getEnvironment().getIOManager();

		NormalizedKeyComputer computer = computerClass.newInstance();
		RecordComparator comparator = comparatorClass.newInstance();
		computer.init(gSorter.serializers(), gSorter.comparators());
		comparator.init(gSorter.serializers(), gSorter.comparators());
		this.sorter = new BinaryExternalSorter(this.getContainingTask(), memManager, reservedMemorySize,
				maxMemorySize, perRequestMemorySize, ioManager, inputSerializer, binarySerializer,
				computer, comparator, getSqlConf());
		this.sorter.startThreads();
		gSorter = null;

		collector = new StreamRecordCollector<>(output);

		//register the the metrics.
		getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
	}

	protected void cookGeneratedClasses(ClassLoader cl) throws CompileException {
		computerClass = compile(cl, gSorter.computer().name(), gSorter.computer().code());
		comparatorClass = compile(cl, gSorter.comparator().name(), gSorter.comparator().code());
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		this.sorter.write(element.getValue());
	}

	@Override
	public void endInput() throws Exception {
		BinaryRow row = binarySerializer.createInstance();
		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();
		while ((row = iterator.next(row)) != null) {
			collector.collect(row);
		}
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing SortOperator");
		super.close();
		if (sorter != null) {
			sorter.close();
		}
	}
}
