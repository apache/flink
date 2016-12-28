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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.commons.math3.util.ArithmeticUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.MathUtils;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static java.util.Objects.requireNonNull;

@Internal
@Deprecated
public abstract class AbstractAlignedProcessingTimeWindowOperator<KEY, IN, OUT, STATE, F extends Function> 
		extends AbstractUdfStreamOperator<OUT, F> 
		implements OneInputStreamOperator<IN, OUT>, ProcessingTimeCallback {
	
	private static final long serialVersionUID = 3245500864882459867L;
	
	private static final long MIN_SLIDE_TIME = 50;
	
	// ----- fields for operator parametrization -----
	
	private final Function function;
	private final KeySelector<IN, KEY> keySelector;
	
	private final TypeSerializer<KEY> keySerializer;
	private final TypeSerializer<STATE> stateTypeSerializer;
	
	private final long windowSize;
	private final long windowSlide;
	private final long paneSize;
	private final int numPanesPerWindow;
	
	// ----- fields for operator functionality -----
	
	private transient AbstractKeyedTimePanes<IN, KEY, STATE, OUT> panes;
	
	private transient TimestampedCollector<OUT> out;
	
	private transient RestoredState<IN, KEY, STATE, OUT> restoredState;
	
	private transient long nextEvaluationTime;
	private transient long nextSlideTime;
	
	protected AbstractAlignedProcessingTimeWindowOperator(
			F function,
			KeySelector<IN, KEY> keySelector,
			TypeSerializer<KEY> keySerializer,
			TypeSerializer<STATE> stateTypeSerializer,
			long windowLength,
			long windowSlide)
	{
		super(function);
		
		if (windowLength < MIN_SLIDE_TIME) {
			throw new IllegalArgumentException("Window length must be at least " + MIN_SLIDE_TIME + " msecs");
		}
		if (windowSlide < MIN_SLIDE_TIME) {
			throw new IllegalArgumentException("Window slide must be at least " + MIN_SLIDE_TIME + " msecs");
		}
		if (windowLength < windowSlide) {
			throw new IllegalArgumentException("The window size must be larger than the window slide");
		}
		
		final long paneSlide = ArithmeticUtils.gcd(windowLength, windowSlide);
		if (paneSlide < MIN_SLIDE_TIME) {
			throw new IllegalArgumentException(String.format(
					"Cannot compute window of size %d msecs sliding by %d msecs. " +
							"The unit of grouping is too small: %d msecs", windowLength, windowSlide, paneSlide));
		}
		
		this.function = requireNonNull(function);
		this.keySelector = requireNonNull(keySelector);
		this.keySerializer = requireNonNull(keySerializer);
		this.stateTypeSerializer = requireNonNull(stateTypeSerializer);
		this.windowSize = windowLength;
		this.windowSlide = windowSlide;
		this.paneSize = paneSlide;
		this.numPanesPerWindow = MathUtils.checkedDownCast(windowLength / paneSlide);
	}
	
	
	protected abstract AbstractKeyedTimePanes<IN, KEY, STATE, OUT> createPanes(
			KeySelector<IN, KEY> keySelector, Function function);

	// ------------------------------------------------------------------------
	//  startup and shutdown
	// ------------------------------------------------------------------------

	@Override
	public void open() throws Exception {
		super.open();

		out = new TimestampedCollector<>(output);
		
		// decide when to first compute the window and when to slide it
		// the values should align with the start of time (that is, the UNIX epoch, not the big bang)
		final long now = getProcessingTimeService().getCurrentProcessingTime();
		nextEvaluationTime = now + windowSlide - (now % windowSlide);
		nextSlideTime = now + paneSize - (now % paneSize);

		final long firstTriggerTime = Math.min(nextEvaluationTime, nextSlideTime);
		
		// check if we restored state and if we need to fire some windows based on that restored state
		if (restoredState == null) {
			// initial empty state: create empty panes that gather the elements per slide
			panes = createPanes(keySelector, function);
		}
		else {
			// restored state
			panes = restoredState.panes;
			
			long nextPastEvaluationTime = restoredState.nextEvaluationTime;
			long nextPastSlideTime = restoredState.nextSlideTime;
			long nextPastTriggerTime = Math.min(nextPastEvaluationTime, nextPastSlideTime);
			int numPanesRestored = panes.getNumPanes();
			
			// fire windows from the past as long as there are more panes with data and as long
			// as the missed trigger times have not caught up with the presence
			while (numPanesRestored > 0 && nextPastTriggerTime < firstTriggerTime) {
				// evaluate the window from the past
				if (nextPastTriggerTime == nextPastEvaluationTime) {
					computeWindow(nextPastTriggerTime);
					nextPastEvaluationTime += windowSlide;
				}
				
				// evaluate slide from the past
				if (nextPastTriggerTime == nextPastSlideTime) {
					panes.slidePanes(numPanesPerWindow);
					numPanesRestored--;
					nextPastSlideTime += paneSize;
				}

				nextPastTriggerTime = Math.min(nextPastEvaluationTime, nextPastSlideTime);
			}
		}

		// make sure the first window happens
		getProcessingTimeService().registerTimer(firstTriggerTime, this);
	}

	@Override
	public void close() throws Exception {
		super.close();
		
		// early stop the triggering thread, so it does not attempt to return any more data
		stopTriggers();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		
		// acquire the lock during shutdown, to prevent trigger calls at the same time
		// fail-safe stop of the triggering thread (in case of an error)
		stopTriggers();

		// release the panes. panes may still be null if dispose is called
		// after open() failed
		if (panes != null) {
			panes.dispose();
		}
	}
	
	private void stopTriggers() {
		// reset the action timestamps. this makes sure any pending triggers will not evaluate
		nextEvaluationTime = -1L;
		nextSlideTime = -1L;
	}

	// ------------------------------------------------------------------------
	//  Receiving elements and triggers
	// ------------------------------------------------------------------------
	
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		panes.addElementToLatestPane(element.getValue());
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		// first we check if we actually trigger the window function
		if (timestamp == nextEvaluationTime) {
			// compute and output the results
			computeWindow(timestamp);

			nextEvaluationTime += windowSlide;
		}

		// check if we slide the panes by one. this may happen in addition to the
		// window computation, or just by itself
		if (timestamp == nextSlideTime) {
			panes.slidePanes(numPanesPerWindow);
			nextSlideTime += paneSize;
		}

		long nextTriggerTime = Math.min(nextEvaluationTime, nextSlideTime);
		getProcessingTimeService().registerTimer(nextTriggerTime, this);
	}
	
	private void computeWindow(long timestamp) throws Exception {
		out.setAbsoluteTimestamp(timestamp);
		panes.truncatePanes(numPanesPerWindow);
		panes.evaluateWindow(out, new TimeWindow(timestamp - windowSize, timestamp), this);
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		super.snapshotState(out, checkpointId, timestamp);

		// we write the panes with the key/value maps into the stream, as well as when this state
		// should have triggered and slided

		DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(out);

		outView.writeLong(nextEvaluationTime);
		outView.writeLong(nextSlideTime);

		panes.writeToOutput(outView, keySerializer, stateTypeSerializer);

		outView.flush();
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		super.restoreState(in);

		DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(in);

		final long nextEvaluationTime = inView.readLong();
		final long nextSlideTime = inView.readLong();

		AbstractKeyedTimePanes<IN, KEY, STATE, OUT> panes = createPanes(keySelector, function);

		panes.readFromInput(inView, keySerializer, stateTypeSerializer);

		restoredState = new RestoredState<>(panes, nextEvaluationTime, nextSlideTime);
	}

	// ------------------------------------------------------------------------
	//  Property access (for testing)
	// ------------------------------------------------------------------------

	public long getWindowSize() {
		return windowSize;
	}

	public long getWindowSlide() {
		return windowSlide;
	}

	public long getPaneSize() {
		return paneSize;
	}
	
	public int getNumPanesPerWindow() {
		return numPanesPerWindow;
	}

	public long getNextEvaluationTime() {
		return nextEvaluationTime;
	}

	public long getNextSlideTime() {
		return nextSlideTime;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Window (processing time) (length=" + windowSize + ", slide=" + windowSlide + ')';
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	
	private static final class RestoredState<IN, KEY, STATE, OUT> {

		final AbstractKeyedTimePanes<IN, KEY, STATE, OUT> panes;
		final long nextEvaluationTime;
		final long nextSlideTime;

		RestoredState(AbstractKeyedTimePanes<IN, KEY, STATE, OUT> panes, long nextEvaluationTime, long nextSlideTime) {
			this.panes = panes;
			this.nextEvaluationTime = nextEvaluationTime;
			this.nextSlideTime = nextSlideTime;
		}
	}
}
