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


function convertHex(hex,opacity){
    hex = hex.replace('#','');
    r = parseInt(hex.substring(0,2), 16);
    g = parseInt(hex.substring(2,4), 16);
    b = parseInt(hex.substring(4,6), 16);

    result = 'rgba('+r+','+g+','+b+','+opacity/100+')';
    return result;
}

/**
 * Copied and modified from: https://github.com/shutterstock/rickshaw/blob/master/src/js/Rickshaw.Fixtures.Number.js
 **/
var formatBase1024KMGTP = function(y) {
    var abs_y = Math.abs(y);
    if (abs_y >= 1125899906842624)  { return Math.floor(y / 1125899906842624) + "P" }
    else if (abs_y >= 1099511627776){ return Math.floor(y / 1099511627776) + "T" }
    else if (abs_y >= 1073741824)   { return Math.floor(y / 1073741824) + "G" }
    else if (abs_y >= 1048576)      { return Math.floor(y / 1048576) + "M" }
    else if (abs_y >= 1024)         { return Math.floor(y / 1024) + "K" }
    else if (abs_y < 1 && y > 0)    { return y.toFixed(2) }
    else if (abs_y === 0)           { return '' }
    else                        { return y }
};

function getUnixTime() {
	return Math.floor(new Date().getTime()/1000);
}

// this array contains the history metrics for the taskManagers.
var taskManagerMemory = [];

// array with the graphs for each taskManager.
var taskManagerGraph = [];

// array with the latest metrics for all TaskManagers (for the Full metrics view)
var taskManagerMetrics = [];

// values for the memory charting. In order!
var memoryValues = ["memory.non-heap.used" , "memory.flink.used", "memory.heap.used" ];

var metricsLimit = 3;

/**
Create rickshaw graph for the specified taskManager id (tmid).
**/
function createGraph(tmId, maxload, maxmem) {
    var palette = new Rickshaw.Color.Palette({scheme: "spectrum14"} );
    var series = [];
    var scales = [];
    scales.push(d3.scale.linear().domain([0, maxmem]));
    scales.push(d3.scale.linear().domain([0, maxload]).nice());
    for(i in memoryValues) {
        var value = memoryValues[i];
        taskManagerMemory[tmId][value] = [];
        series.push({
            color: convertHex(palette.color(), 90),
            data: taskManagerMemory[tmId][value],
            name: value,
            scale: scales[0],
            renderer: 'area',
            stroke: 'rgba(0,0,0,0.5)'
        });
    }
    taskManagerMemory[tmId]["load"] = [];
    // add load series
    series.push({
        color: palette.color(),
        scale: scales[1],
        data: taskManagerMemory[tmId]["load"],
        name: "OS Load",
        renderer: 'line',
        stroke: 'rgba(0,0,0,0.5)'
    });

    // remove message
    $("#chart-"+tmId).html("");
    var graph = new Rickshaw.Graph( {
        element: document.querySelector("#chart-"+tmId),
        width: 560,
        height: 250,
        series: series,
        renderer: 'multi',
        stroke: true,
        min: 0,
        max: 1
    } );

    var x_axis = new Rickshaw.Graph.Axis.Time( { graph: graph } );

    var y_axis = new Rickshaw.Graph.Axis.Y.Scaled( {
        graph: graph,
        orientation: 'left',
        scale: scales[0],
        height: 250,
        pixelsPerTick: 30,
        tickSize: 1,
        tickFormat: formatBase1024KMGTP,
        element: document.getElementById("y_axis-"+tmId)
    } );

    var y_axis_load = new Rickshaw.Graph.Axis.Y.Scaled( {
        graph: graph,
        orientation: 'right',
        scale: scales[1],
        grid: false,
        element: document.getElementById("y_axis-load-"+tmId)
    } );

    var hoverDetail = new Rickshaw.Graph.HoverDetail( {
        graph: graph,
        yFormatter: formatBase1024KMGTP
    } );

    var legend = new Rickshaw.Graph.Legend({
        graph: graph,
        element: document.querySelector("#legend-"+tmId)
    });

    var tableBox = $("#tm-row-"+tmId+"-memory");

    // make graph resizable
    var resize = function() {
        graph.configure({
            width: tableBox.innerWidth() - $(".y_axis").width() - 80
        });
        graph.render();
    }
    setTimeout(resize, 1000);
    resize();
    window.addEventListener('resize', resize);

    return graph;
}

function drawOrUpdateGCStats(tmId, metrics) {
    var gcs = [];
    for(var key in metrics.gauges) {
        var pat = /gc.([^.]+).(count|time)/
        if(pat.test(key)) {
            var matches = key.match(pat);
            if($.inArray(matches[1], gcs) == -1) {
                gcs.push(matches[1]);
            }
        }
    }

    var html =  "<table class=\"table table-bordered table-hover table-striped\">"+
                "<tr><td>Name</td><td>Count</td><td>Time</td></tr>";
    for(var key in gcs) {
        var gc = gcs[key];
        html += "<tr><td>"+gc+"</td>";
        html += "<td>"+metrics.gauges["gc."+gc+".count"].value+"</td>";
        html += "<td>"+metrics.gauges["gc."+gc+".time"].value+" ms</td></tr>";
    }
    html +="</table>";
    $("#gcStats-"+tmId).html(html);
}

function getTooltipHTML(txt) {
    return "<i class=\"fa fa-exclamation-circle\" data-toggle=\"tooltip\" data-placement=\"top\" title=\""+txt+"\"></i>";
}

/*
 * Initializes taskmanagers table
 */
function processTMdata(json) {
    var tableHeader = $("#taskmanagerTable-header");
    $("#page-title").text("Task Managers ("+json.taskmanagers.length+")");
	for (var i = 0; i < json.taskmanagers.length; i++) {
		var tm = json.taskmanagers[i];
		var tmRowIdCssName = "tm-row-"+tm.instanceID;
		if(!tm.hasOwnProperty("metrics")) {
		    // metrics not yet received by the JobManager
		    return;
		}
		var metricsJSON = tm.metrics;
		taskManagerMetrics[tm.instanceID] = metricsJSON;

		// check if taskManager has a row
		tmRow = $("#"+tmRowIdCssName);
		if(tmRow.length == 0) {
		    var tmMemoryBox = "<div class=\"chart_container\" id=\"chart_container-"+tm.instanceID+"\">"+
                                  "<div class=\"y_axis\" id=\"y_axis-"+tm.instanceID+"\"><p class=\"axis_label\">Memory</p></div>"+
                                  "<div class=\"chart\" id=\"chart-"+tm.instanceID+"\"><i>Waiting for first Heartbeat to arrive</i></div>"+
                                  "<div class=\"y_axis-load\" id=\"y_axis-load-"+tm.instanceID+"\"><p class=\"axis_label\">Load</p></div>"+
                               "<div class=\"legend\" id=\"legend-"+tm.instanceID+"\"></div>"+
                               "</div>";

            var content = "<tr id=\""+tmRowIdCssName+"\">" +
		                "<td style=\"width:20%\">"+tm.inetAdress+" <br> IPC Port: "+tm.ipcPort+", Data Port: "+tm.dataPort+"</td>" + // first row: TaskManager
		                "<td id=\""+tmRowIdCssName+"-memory\">"+tmMemoryBox+"</td>" + // second row: memory statistics
		                "<td id=\""+tmRowIdCssName+"-info\"><i>Loading Information</i></td>" + // Information
		                "</tr>";
            var siblings = tableHeader.siblings();
            if(siblings.length == 0) {
                tableHeader.after(content);
            } else {
                var f = siblings.last();
                f.after(content);
            }
		    var maxmem = metricsJSON.gauges["memory.total.max"].value;
		    taskManagerMemory[tm.instanceID] = []; // create empty array for TM
		    taskManagerGraph[tm.instanceID] = createGraph(tm.instanceID, tm.cpuCores*2, maxmem); // cpu cores as load approximation
		    taskManagerGraph[tm.instanceID].render();
        //    taskManagerGraph[tm.instanceID].resize();
		}
        // fill (update) row with contents
        // memory statistics
        var time = getUnixTime();
        for(memValIdx in memoryValues) {
            valueKey = memoryValues[memValIdx];

            var flinkMemory = tm.managedMemory * 1024 * 1024;
            switch(valueKey) {
                case "memory.heap.used":
                    var value = metricsJSON.gauges[valueKey].value - flinkMemory;
                    break;
                case "memory.non-heap.used":
                    var value = metricsJSON.gauges[valueKey].value;
                    break;
                case "memory.flink.used":
                    var value = flinkMemory;
                    break;
            }
            taskManagerMemory[tm.instanceID][valueKey].push({x: time, y: value})
        }
        // load
        taskManagerMemory[tm.instanceID]["load"].push({x:time, y:metricsJSON.gauges["load"].value });

        if(metricsLimit == -1 || i < metricsLimit) {
            taskManagerGraph[tm.instanceID].update();
        } else {
            $("#chart_container-"+tm.instanceID).hide();
        }



        // info box
        tmInfoBox = $("#"+tmRowIdCssName+"-info");
        var slotsInfo = "";
        if(tm.slotsNumber < tm.cpuCores) {
            slotsInfo = getTooltipHTML("The number of configured processing slots ("+tm.slotsNumber+") is lower than the "+
                "number of CPU cores ("+tm.cpuCores+"). For good performance, the number of slots should be at least the number of cores.");
        }
        var memoryInfo = "";
        if(  (tm.managedMemory/tm.physicalMemory) < 0.6 ) {
            memoryInfo = getTooltipHTML("The amout of memory available to Flink ("+tm.managedMemory+" MB) is much lower than "+
                "the physical memory available on the machine ("+tm.physicalMemory+" MB). For good performance, Flink should get as much memory as possible.");
        }
        tmInfoBox.html("Last Heartbeat: "+tm.timeSinceLastHeartbeat+" seconds ago<br>"+
            "Processing Slots: "+tm.freeSlots+"/"+tm.slotsNumber+" "+slotsInfo+"<br>"+
            "Flink Managed Memory: "+tm.managedMemory+" mb "+memoryInfo+"<br>"+
            "CPU cores: "+tm.cpuCores+" <br>"+
            "Physical Memory "+tm.physicalMemory+" mb"+
            "<div id=\"gcStats-"+tm.instanceID+"\"></div>"+
            "<button type=\"button\" class=\"btn btn-default\" onclick=\"javascript:showStacktraceOfTaskmanager('"+ tm.instanceID +"')\">Show Stacktrace</button> "+
            "<button type=\"button\" class=\"btn btn-default\" onclick=\"javascript:showAllMetrics('"+ tm.instanceID +"')\">Show all metrics</button>");
        $(function () {
            $('[data-toggle="tooltip"]').tooltip()
        });
        drawOrUpdateGCStats(tm.instanceID, metricsJSON);

	}
}

function showStacktraceOfTaskmanager(instanceId) {
    $.ajax({
        url: "setupInfo?get=stackTrace&instanceID=" + instanceId,
        type: "GET",
        cache: false,
        dataType: "json",
        success: function(json) {
            var html = "<h2>Stack Trace of TaskManager ("+ instanceId +")</h2>";
            if ("stackTrace" in json) {
                html += "<pre>" + json.stackTrace + "</pre>";
            } else if ("errorMessage" in json) {
                html += "<pre>" + json.errorMessage + "</pre>";
            }
            $("#taskManagerStackTrace").parent().show();
            $("#taskManagerStackTrace").html(html);
        }
    });
}

function showAllMetrics(instanceID) {
    $("#allMetrics").parent().show();
    $("#allMetrics").html("<h1>All metrics</h1><pre>"+JSON.stringify(taskManagerMetrics[instanceID], undefined, 2)+"</pre>");
}


function updateLimit(element) {
    switch(element.id) {
        case 'metrics-limit-3':
            $("#metrics-limit-all,#metrics-limit-none").removeClass("active");
            $(element).addClass("active");
            metricsLimit = 3;
            hideShowGraphs();
            break;
        case 'metrics-limit-all':
            $("#metrics-limit-3,#metrics-limit-none").removeClass("active");
            $(element).addClass("active");
            metricsLimit = -1;
            hideShowGraphs();
            break;
        case 'metrics-limit-none':
            $("#metrics-limit-all,#metrics-limit-3").removeClass("active");
            $(element).addClass("active");
            metricsLimit = 0;
            hideShowGraphs();
            break;
    }
}

function hideShowGraphs() {
    var i = 0;
    for(tmid in taskManagerMemory) {
       if(metricsLimit == -1 || i++ < metricsLimit) {
            $("#chart_container-"+tmid).show();
       } else {
            $("#chart_container-"+tmid).hide();
       }
    }
}


function updateTaskManagers() {
	$.ajax({ url : "setupInfo?get=taskmanagers", type : "GET", cache: false, success : function(json) {
		processTMdata(json);
	}, dataType : "json"
	});
}


$(document).ready(function() {
    updateTaskManagers(); // first call
	setInterval(updateTaskManagers, 5000); // schedule periodic calls.
});
