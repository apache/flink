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

/**
 * Template for profiling data.
 */
function ProfilingSeries(label, number) {
	this.lastTimestamp = 0;
	this.flotData = {
		color : number,
		label : label,
		data : []
	};
}

/** Container for the different profiling series. */
var collectedProfilingData = {};
$(function initResourceUsage() {
	collectedProfilingData.freeMemory = new ProfilingSeries("Free memory", 0);
	collectedProfilingData.userCpu = new ProfilingSeries("User CPU", 1);
	collectedProfilingData.transmittedBytes = new ProfilingSeries(
			"Transmitted bytes", 2);
});


/** Options to be used with flot. */
function makeMemorySizeFormatter(baseScale) {
	return function(val, axis) {
		val *= baseScale
		if (val > 1000000)
			return (val / 1000000).toFixed(axis.tickDecimals) + " MB";
		else if (val > 1000)
			return (val / 1000).toFixed(axis.tickDecimals) + " kB";
		else
			return val.toFixed(axis.tickDecimals) + " B";
	}
}

var mainMemoryFlotOptions = {
	xaxis : {
		mode : "time",
		timeformat : "%H:%M:%S",
	},
	yaxis : {
		min : 0,
		// /proc/meminfo delivers kiB
		tickFormatter : makeMemorySizeFormatter(1024)
	}
}

var networkMemoryFlotOptions = {
		xaxis : {
			mode : "time",
			timeformat : "%H:%M:%S",
		},
		yaxis : {
			min : 0,
			tickFormatter : makeMemorySizeFormatter(1)
		}
	}

var cpuFlotOptions = {
	xaxis : {
		mode : "time",
		timeformat : "%H:%M:%S",
	},
	yaxis : {
		min : 0,
		tickFormatter : function(val, axis) {
			return val.toFixed(axis.tickDecimals) + " %"
		}
	}
}


/**
 * Polls profiling events and updates the plot.
 */
function pollResourceUsage() {
	$.ajax({
		url : "resourceUsage",
		cache : false,
		type : "GET",
		success : function(serverResponse) {
			updateData(serverResponse);
			plotData();
		},
		dataType : "json",
	});
};

/** Updates a profiling series with the given profiling event. */
function updateProfilingSeries(series, profilingEvent, property) {
	if (profilingEvent.timestamp > series.lastTimestamp) {
		series.lastTimestamp = profilingEvent.timestamp;
		series.flotData.data.push([ profilingEvent.timestamp,
				profilingEvent[property] ]);
	}
}

/** Updates all the profiling series from an array of profiling events. */
function updateData(serverResponse) {
	for ( var eventIndex in serverResponse) {
		var profilingEvent = serverResponse[eventIndex];
		console.log(profilingEvent);
		console.log(profilingEvent.type);
		if (profilingEvent.type == "InstanceSummaryProfilingEvent") {
			updateProfilingSeries(collectedProfilingData.userCpu,
					profilingEvent, "userCpu");
			updateProfilingSeries(collectedProfilingData.transmittedBytes,
					profilingEvent, "transmittedBytes");
			updateProfilingSeries(collectedProfilingData.freeMemory,
					profilingEvent, "freeMemory");
		}
	}
}

function plotData() {
	$.plot("#cpuChart", [ collectedProfilingData.userCpu.flotData ],
			cpuFlotOptions);
	$.plot("#memoryChart", [ collectedProfilingData.freeMemory.flotData ],
			mainMemoryFlotOptions);
	$.plot("#networkChart",
			[ collectedProfilingData.transmittedBytes.flotData ], networkMemoryFlotOptions);
}

$(function startPolling() {
	pollResourceUsage();
	setInterval(pollResourceUsage, 1000);
})
