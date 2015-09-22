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

package org.apache.flink.streaming.runtime.operators.windows;

import org.apache.commons.math3.util.ArithmeticUtils;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.TriggerTimer;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;


public abstract class AbstractAlignedProcessingTimeWindowOperator<KEY, IN, OUT> extends AbstractStreamOperator<OUT> 
		implements OneInputStreamOperator<IN, OUT>, Triggerable {
	
	private static final long serialVersionUID = 3245500864882459867L;
	
	private static final long MIN_SLIDE_TIME = 50;
	
	
	private final SerializableObject lock = new SerializableObject();

	// ----- fields for operator parametrization -----
	
	private final Function function;
	private final KeySelector<IN, KEY> keySelector;
	
	private final long windowSize;
	private final long windowSlide;
	private final long paneSize;
	private final int numPanesPerWindow;
	
	// ----- fields for operator functionality -----
	
	private transient AbstractKeyedTimePanes<IN, KEY, ?, OUT> panes;
	
	private transient TimestampedCollector<OUT> out;
	
	private transient TriggerTimer triggerTimer;
	
	private transient long nextEvaluationTime;
	private transient long nextSlideTime;
	
	private transient volatile Throwable asyncError;
	
	
	protected AbstractAlignedProcessingTimeWindowOperator(
			Function function,
			KeySelector<IN, KEY> keySelector,
			long windowLength,
			long windowSlide)
	{
		if (function == null || keySelector == null) {
			throw new NullPointerException();
		}
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
		
		this.function = function;
		this.keySelector = keySelector;
		this.windowSize = windowLength;
		this.windowSlide = windowSlide;
		this.paneSize = paneSlide;
		this.numPanesPerWindow = MathUtils.checkedDownCast(windowLength / paneSlide);
	}
	
	
	protected abstract AbstractKeyedTimePanes<IN, KEY, ?, OUT> createPanes(
			KeySelector<IN, KEY> keySelector, Function function);

	// ------------------------------------------------------------------------
	//  startup and shutdown
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		out = new TimestampedCollector<>(output);
		
		// create the panes that gather the elements per slide
		panes = createPanes(keySelector, function);
		
		// decide when to first compute the window and when to slide it
		// the values should align with the start of time (that is, the UNIX epoch, not the big bang)
		final long now = System.currentTimeMillis();
		nextEvaluationTime = now + windowSlide - (now % windowSlide);
		nextSlideTime = now + paneSize - (now % paneSize);
		
		// start the trigger timer
		triggerTimer = new TriggerTimer("Trigger for " + getRuntimeContext().getTaskName());
		
		// schedule the first trigger
		triggerTimer.scheduleTriggerAt(this, Math.min(nextEvaluationTime, nextSlideTime));
	}

	@Override
	public void close() throws Exception {
		// acquire the lock during shutdown, to prevent trigger calls at the same time
		synchronized (lock) {
			final long finalWindowTimestamp = nextEvaluationTime;
			
			// early stop the triggering thread, so it does not attempt to return any more data
			stopTriggers();

			// make sure we had no asynchronous error so far
			checkErroneous();
			
			// emit the remaining data
			computeWindow(finalWindowTimestamp);
		}
	}

	@Override
	public void dispose() {
		// acquire the lock during shutdown, to prevent trigger calls at the same time
		synchronized (lock) {
			// fail-safe stop of the triggering thread (in case of an error)
			stopTriggers();
			
			// release the panes
			panes.dispose();
		}
	}
	
	private void stopTriggers() {
		if (triggerTimer != null) {
			triggerTimer.shutdown();
		}

		// reset the action timestamps. this makes sure any pending triggers will not evaluate
		nextEvaluationTime = -1L;
		nextSlideTime = -1L;
	}

	// ------------------------------------------------------------------------
	//  Receiving elements and triggers
	// ------------------------------------------------------------------------
	
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		synchronized (lock) {
			checkErroneous();
			panes.addElementToLatestPane(element.getValue());
		}
	}

	@Override
	public void processWatermark(Watermark mark) {
		// this operator does not react to watermarks
	}

	@Override
	public void trigger(long timestamp) {
		synchronized (lock) {
			// first we check if we actually trigger the window function
			if (timestamp == nextEvaluationTime) {
				// compute and output the results
				try {
					computeWindow(timestamp);
				}
				catch (Throwable t) {
					this.asyncError = t;
				}

				nextEvaluationTime += windowSlide;
			}
			
			// check if we slide the panes by one. this may happen in addition to the
			// window computation, or just by itself
			if (timestamp == nextSlideTime) {
				try {
					panes.slidePanes(numPanesPerWindow);
				}
				catch (Throwable t) {
					this.asyncError = t;
				}
				nextSlideTime += paneSize;
			}
			
			long nextTriggerTime = Math.min(nextEvaluationTime, nextSlideTime);
			triggerTimer.scheduleTriggerAt(this, nextTriggerTime);
		}
	}
	
	private void checkErroneous() throws Exception {
		if (asyncError != null) {
			throw new Exception("Error while computing and producing window result", asyncError);
		}
	}
	
	private void computeWindow(long timestamp) throws Exception {
		out.setTimestamp(timestamp);
		panes.truncatePanes(numPanesPerWindow);
		panes.evaluateWindow(out);
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
}
