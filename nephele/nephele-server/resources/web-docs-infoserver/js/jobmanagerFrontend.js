var jsonGlobal;
var widthProgressbar = 120;

// For coloring the dependency graph
var colors = [ "#37485D", "#D9AADC", "#4F7C61", "#8F9C6A", "#BC8E88" ];

(function poll() {
	$.ajax({ url : "/jobsInfo", type : "GET", success : function(json) {
	    jsonGlobal = json
	    // Fill Table	
	    fillTable("#jobs", json)
	}, dataType : "json",
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
})();

(function pollArchive() {
	$.ajax({ url : "/jobsInfo?get=archive", type : "GET",
	    success : function(json) {
		// Fill Table	
		fillTableArchive("#jobsArchive", json)
	    }, dataType : "json",
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
})();

(function pollTaskmanagers() {
	$.ajax({ url : "/jobsInfo?get=taskmanagers", type : "GET",
	    success : function(json) {
		$("#stat-taskmanagers").html(json.taskmanagers);
	    }, dataType : "json",
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
})();

/*
 * Toggle ExecutionVectors
 */
$(".opensub").live("click", function() {
	var id = $(this).attr("open");
	$("#" + id).toggle();
	drawDependencies();
});

$(".cancel").live("click", function() {
	var id = $(this).attr("job");
	$.ajax({ url : "/jobsInfo?get=cancel&job=" + id, type : "GET",
	    success : function(json) {
	    }
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
});

/*
 * Draw graph on left side beside table
 */
function drawDependencies() {
	$.each(jsonGlobal, function(i, job) {
		$("#dependencies" + job.jobid).clearCanvas();
		$("#dependencies" + job.jobid).attr("height", 10);
		$("#dependencies" + job.jobid).attr("height", ($("#" + job.jobid).height()));

		//var colors = randomColors(job.groupvertices.length+1)
		edgeCount = -1;
		$.each(job.groupvertices, function(j, groupvertex) {
			$.each(groupvertex.backwardEdges, function(k, edge) {
				var y1 = ($("#" + edge.groupvertexid).offset().top - $("#dependencies"+ job.jobid).offset().top) + 15;
				var y2 = ($("#" + groupvertex.groupvertexid).offset().top - $("#dependencies" + job.jobid).offset().top) + 15;
				var cy1 = y1 + (y2 - y1) / 2;
				//var cx1 = 70 - (edgeCount / groupvertex.numberofgroupmembers) * 140;
				var cx1 = 0;
				edgeCount++;

				var strokeWidth = 2;
				if (edge.channelType == "NETWORK")
					var strokeWidth = 3;

				$("#dependencies" + job.jobid).drawQuadratic({
					strokeStyle : colors[edgeCount % 5],
					strokeWidth : strokeWidth,
					rounded : true,
					endArrow : true,
					arrowRadius : 10,
					arrowAngle : 40,
					x1 : 95,
					y1 : y1, // Start point
					cx1 : cx1,
					cy1 : cy1, // Control point
					x2 : 95,
					y2 : y2
				// End point
				});
			});
		});
	});

}

function fillTable(table, json) {
	$(table).html("");

	$.each(json, function(i, job) {
		var countGroups = 0;
		var countTasks = 0;
		var countStarting = 0;
		var countRunning = 0;
		var countFinished = 0;
		var countCanceled = 0;
		var countFailed = 0;
		$(table).append(
						"<h2>"+ job.jobname
								+ " <span style=\"text-decoration: none\">(time: "+ formattedTimeFromTimestamp(job.time) + ")</span>"
								+"</h2>"
								+"<a class=\"cancel btn\" href=\"#\" job=\""+job.jobid+"\">cancel</a><br />");
		var jobtable;
		jobtable = "<table id=\""+job.jobid+"\">\
						<tr>\
							<th>Name</th>\
							<th>Tasks</th>\
							<th>Starting</th>\
							<th>Running</th>\
							<th>Finished</th>\
							<th>Canceled</th>\
							<th>Failed</th>\
						</tr>";

		$.each(job.groupvertices, function(j, groupvertex) {
			countGroups++;
			countTasks += groupvertex.numberofgroupmembers;
			countStarting += (groupvertex.CREATED + groupvertex.SCHEDULED + groupvertex.ASSIGNED + groupvertex.READY + groupvertex.STARTING);
			countRunning += groupvertex.RUNNING;
			countFinished += groupvertex.FINISHING + groupvertex.FINISHED;
			countCanceled += groupvertex.CANCELING + groupvertex.CANCELED;
			countFailed += groupvertex.FAILED;
			jobtable += "<tr>\
							<td id=\""+groupvertex.groupvertexid+"\">\
								<span class=\"opensub\" open=\"_"+groupvertex.groupvertexid+"\">"
									+ groupvertex.groupvertexname
								+ "</span>\
							</td>\
							<td>"+ groupvertex.numberofgroupmembers+ "</td>\
							<td class=\"starting\"><div class=\"progressBar\"><div style=\"width:"
							+ ((groupvertex.CREATED + groupvertex.SCHEDULED + groupvertex.ASSIGNED + groupvertex.READY + groupvertex.STARTING) / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ (groupvertex.CREATED + groupvertex.SCHEDULED + groupvertex.ASSIGNED + groupvertex.READY + groupvertex.STARTING)
							+ "</div></div></td>\
							<td class=\"running\"><div class=\"progressBar\"><div style=\"width:"+ (groupvertex.RUNNING / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ groupvertex.RUNNING
							+ "</div></div></td>\
							<td class=\"finished\"><div class=\"progressBar\"><div style=\"width:" + ((groupvertex.FINISHING + groupvertex.FINISHED) / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ (groupvertex.FINISHING + groupvertex.FINISHED)
							+ "</div></div></td>\
							<td class=\"canceled\"><div class=\"progressBar\"><div style=\"width:" + ((groupvertex.CANCELING + groupvertex.CANCELED) / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ (groupvertex.CANCELING + groupvertex.CANCELED)
							+ "</div></div></td>\
							<td class=\"failed\"><div class=\"progressBar\"><div style=\"width:"
							+ (groupvertex.FAILED / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ groupvertex.FAILED
							+ "</div></div></td>\
						</tr><tr>\
						<td colspan=8 id=\"_"+groupvertex.groupvertexid+"\" style=\"display:none\">\
							<table class=\"subtable\">\
							  	<tr>\
							  		<th>Name</th>\
							  		<th>status</th>\
							  		<th>instancename</th>\
							  		<th>instancetype</th>\
							  	</tr>";
							$.each(groupvertex.groupmembers, function(k, vertex) {
								jobtable += "<tr>\
       								<td>"+ vertex.vertexname + "</td>\
       								<td>"+ vertex.vertexstatus + "</td>\
       								<td>"+ vertex.vertexinstancename + "</td>\
       								<td>"+ vertex.vertexinstancetype + "</td>\
       							</tr>";
							});
							jobtable += "</table>\
						</td></tr>";
						});

		jobtable += "<tr>\
						<td colspan=\"2\" align=\"center\">Sum</td>\
						<td>"+ countTasks + "</td>\
						<td class=\"starting\"><div class=\"progressBar\"><div style=\"width:" + (countStarting / countTasks) * widthProgressbar + "px\">" 
							+ countStarting
						+ "</div></div></td>\
						<td class=\"running\"><div class=\"progressBar\"><div style=\"width:" + (countRunning / countTasks) * widthProgressbar + "px\">"
							+ countRunning
						+ "</div></div></td>\
						<td class=\"finished\"><div class=\"progressBar\"><div style=\"width:"+ (countFinished / countTasks) * widthProgressbar + "px\">"
							+ countFinished
						+ "</div></div></td>\
						<td class=\"canceled\"><div class=\"progressBar\"><div style=\"width:"+ (countCanceled / countTasks) * widthProgressbar + "px\">"
							+ countCanceled
						+ "</div></div></td>\
						<td class=\"failed\"><div class=\"progressBar\"><div style=\"width:" + (countFailed / countTasks) * widthProgressbar + "px\">"
							+ countFailed
						+ "</div></div></td>\
					</tr>";

		jobtable += "</table>"
		$(table).append(jobtable);
		$("#" + job.jobid).prepend(
						"<tr><td width=\"100\" rowspan=" + (countGroups * 2 + 2)+ ">\
							<canvas id=\"dependencies" + job.jobid+ "\" height=\""+ ($("#" + job.jobid).height())+ "\" width=\"100\"></canvas>\
						</td></tr>");
	});
	drawDependencies(json);

}

function fillTableArchive(table, json) {
	$(table).html("");
	var finished = 0;
	var failed = 0;
	var canceled = 0;
	$.each(json, function(i, job) {
		$(table).append(
				"<li><a href=\"/analyze.html?job=" + job.jobid + "\">"
						+ job.jobname + " (time: "
						+ formattedTimeFromTimestamp(job.time)
						+ ")</a></li>");
		if (job.status == "FINISHED")
			finished++;
		if (job.status == "FAILED")
			failed++;
		if (job.status == "CANCELED")
			canceled++;
	});
	$("#jobs-finished").html(finished);
	$("#jobs-failed").html(failed);
	$("#jobs-canceled").html(canceled);
}