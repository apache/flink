var jsonGlobal;
var widthProgressbar = 120;

// For coloring the dependency graph
var colors = [ "#37485D", "#D9AADC", "#4F7C61", "#8F9C6A", "#BC8E88" ];

var timestamp = 0;

var recentjobs = new Array();

/*
 * Initializes global job table
 */
function init() {
	$.ajax({ url : "jobsInfo", type : "GET", cache: false, success : function(json) {
		
		// If no job running, poll for new jobs
		if(json == "")
			setTimeout(function() {init()}, 2000);
		
	    jsonGlobal = json
	    // Fill Table	
	    fillTable("#jobs", json)
	    
	}, dataType : "json",
	});
}
	
// Init once on page load
$(init());

/*
 * Pools for updates on currently running jobs
 */
function poll(jobId) {
	$.ajax({ url : "jobsInfo?get=updates&job="+jobId, type : "GET", cache: false, success : function(json) {

		// Call init of no more jobs are running
		$.each(json.recentjobs, function(j, job) {
			if(!$.inArray(job.jobid, recentjobs)) {
				init();
			}
		});
		
		updateTable(json);
	}, dataType : "json",
	});
};

/*
 * Polls the job history on page load
 */
(function pollArchive() {
	$.ajax({ url : "jobsInfo?get=archive", cache: false, type : "GET",
	    success : function(json) {
	    	
		// Fill Table	
		fillTableArchive("#jobsArchive", json)
		
	    }, dataType : "json",
	});
})();

/*
 * Polls the number of taskmanagers on page load
 */
(function pollTaskmanagers() {
	$.ajax({ url : "jobsInfo?get=taskmanagers", cache: false, type : "GET",
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

/**
 * Cancels a job
 */
$(".cancel").live("click", function() {
	var id = $(this).attr("job");
	$.ajax({ url : "jobsInfo?get=cancel&job=" + id, cache: false, type : "GET",
	    success : function(json) {
	    }
	});
});

/*
 * Draw graph on left side beside table
 */
function drawDependencies() {
	$.each(jsonGlobal, function(i, job) {
		$("#dependencies" + job.jobid).clearCanvas();
		$("#dependencies" + job.jobid).attr("height", 10);
		$("#dependencies" + job.jobid).attr("height", ($("#" + job.jobid).height()-$("#sum").height()-14));

		edgeCount = -1;
		$.each(job.groupvertices, function(j, groupvertex) {
			$.each(groupvertex.backwardEdges, function(k, edge) {
				var y1 = ($("#" + edge.groupvertexid).offset().top - $("#dependencies"+ job.jobid).offset().top) + 15;
				var y2 = ($("#" + groupvertex.groupvertexid).offset().top - $("#dependencies" + job.jobid).offset().top) + 15;
				var cy1 = y1 + (y2 - y1) / 2;
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

/*
 * Creates and fills the global running jobs table
 */
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
		recentjobs.push(job.jobid);
		poll(job.jobid);
		if(parseInt(job.time) > timestamp)
			timestamp = parseInt(job.time);
		$(table).append(
						"<h2 id=\""+job.jobid+"_title\">"+ job.jobname
								+ " (time: "+ formattedTimeFromTimestamp(job.time) + ")"
								+"</h2>"
								+"<a id=\""+job.jobid+"_cancel\" class=\"cancel btn\" href=\"#\" job=\""+job.jobid+"\">cancel</a><br />");
		var jobtable;
		jobtable = "<table id=\""+job.jobid+"\" jobname=\""+job.jobname+"\">\
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
			starting = (groupvertex.CREATED + groupvertex.SCHEDULED + groupvertex.ASSIGNED + groupvertex.READY + groupvertex.STARTING);
			countStarting += starting;
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
							<td class=\"nummembers\">"+ groupvertex.numberofgroupmembers+ "</td>\
							<td class=\"starting\" val=\""+starting+"\"><div class=\"progressBar\"><div class=\"progressBarInner\" style=\"width:"
							+ (starting / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ starting
							+ "</div></div></td>\
							<td class=\"running\" val=\""+groupvertex.RUNNING+"\"><div class=\"progressBar\"><div class=\"progressBarInner\" style=\"width:"+ (groupvertex.RUNNING / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ groupvertex.RUNNING
							+ "</div></div></td>\
							<td class=\"finished\" val=\""+(groupvertex.FINISHING + groupvertex.FINISHED)+"\"><div class=\"progressBar\"><div class=\"progressBarInner\" style=\"width:" + ((groupvertex.FINISHING + groupvertex.FINISHED) / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ (groupvertex.FINISHING + groupvertex.FINISHED)
							+ "</div></div></td>\
							<td class=\"canceled\" val=\""+(groupvertex.CANCELING + groupvertex.CANCELED)+"\"><div class=\"progressBar\"><div class=\"progressBarInner\" style=\"width:" + ((groupvertex.CANCELING + groupvertex.CANCELED) / groupvertex.numberofgroupmembers) * widthProgressbar + "px\">"
							+ (groupvertex.CANCELING + groupvertex.CANCELED)
							+ "</div></div></td>\
							<td class=\"failed\" val=\""+groupvertex.FAILED+"\"><div class=\"progressBar\"><div class=\"progressBarInner\" style=\"width:"
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
								jobtable += "<tr id="+vertex.vertexid+" lastupdate=\""+job.time+"\">\
       								<td>"+ vertex.vertexname + "</td>\
       								<td class=\"status\">"+ vertex.vertexstatus + "</td>\
       								<td>"+ vertex.vertexinstancename + "</td>\
       								<td>"+ vertex.vertexinstancetype + "</td>\
       							</tr>";
							});
							jobtable += "</table>\
						</td></tr>";
						});

		jobtable += "<tr id=\"sum\">\
						<td colspan=\"2\" align=\"center\">Sum</td>\
						<td class=\"nummebembers\">"+ countTasks + "</td>\
						<td class=\"starting\" val=\""+countStarting+"\"><div class=\"progressBar\"><div style=\"width:" + (countStarting / countTasks) * widthProgressbar + "px\">" 
							+ countStarting
						+ "</div></div></td>\
						<td class=\"running\"  val=\""+countRunning+"\"><div class=\"progressBar\"><div style=\"width:" + (countRunning / countTasks) * widthProgressbar + "px\">"
							+ countRunning
						+ "</div></div></td>\
						<td class=\"finished\"  val=\""+countFinished+"\"><div class=\"progressBar\"><div style=\"width:"+ (countFinished / countTasks) * widthProgressbar + "px\">"
							+ countFinished
						+ "</div></div></td>\
						<td class=\"canceled\"  val=\""+countCanceled+"\"><div class=\"progressBar\"><div style=\"width:"+ (countCanceled / countTasks) * widthProgressbar + "px\">"
							+ countCanceled
						+ "</div></div></td>\
						<td class=\"failed\"  val=\""+countFailed+"\"><div class=\"progressBar\"><div style=\"width:" + (countFailed / countTasks) * widthProgressbar + "px\">"
							+ countFailed
						+ "</div></div></td>\
					</tr>";

		jobtable += "</table>"
		$(table).append(jobtable);
		$("#" + job.jobid).prepend(
						"<tr><td width=\"100\" rowspan=" + (countGroups * 2 + 2)+ " style=\"overflow:hidden\">\
							<canvas id=\"dependencies" + job.jobid+ "\" height=\"10\" width=\"100\"></canvas>\
						</td></tr>");
	});
	drawDependencies(json);

}

/*
 * Updates the global running job table with newest events
 */
function updateTable(json) {
	var pollfinished = false;
	$.each(json.vertexevents , function(i, event) {

		if(parseInt($("#"+event.vertexid).attr("lastupdate")) < event.timestamp)
		{
			// not very nice
			var oldstatus = ""+$("#"+event.vertexid).children(".status").html();
			if(oldstatus == "CREATED" ||  oldstatus == "SCHEDULED" ||oldstatus == "ASSIGNED" ||oldstatus == "READY" ||oldstatus == "STARTING")
				oldstatus = "starting";
			else if(oldstatus == "FINISHING")
				oldstatus = "finished";
			else if(oldstatus == "CANCELING")
				oldstatus = "canceled";
			
			var newstate = event.newstate;
			if(newstate == "CREATED" ||  newstate == "SCHEDULED" ||newstate == "ASSIGNED" ||newstate == "READY" ||newstate == "STARTING")
				newstate = "starting";
			else if(newstate == "FINISHING")
				newstate = "finished";
			else if(newstate == "CANCELING")
				newstate = "canceled";
			
			// update detailed state
			$("#"+event.vertexid).children(".status").html(event.newstate);
			// update timestamp
			$("#"+event.vertexid).attr("lastupdate", event.timestamp);
			
			var nummembers = parseInt($("#"+event.vertexid).parent().parent().parent().parent().prev().children(".nummembers").html());
			var summembers = parseInt($("#sum").children(".nummebembers").html());
			

			if(oldstatus.toLowerCase() != newstate.toLowerCase()) {
			
				// adjust groupvertex
				var oldcount = parseInt($("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).attr("val"));
				var oldcount2 = parseInt($("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).attr("val"));
				$("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).attr("val", oldcount-1);
				$("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).attr("val", oldcount2+1);
				
				// adjust progressbars
				$("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).first().children().first().children().first().css("width", ((oldcount-1) / nummembers * widthProgressbar)+"px").html(oldcount-1);
				$("#"+event.vertexid).parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).first().children().first().children().first().css("width", ((oldcount2+1) / nummembers * widthProgressbar)+"px").html(oldcount2+1);
				
				
				// adjust sum
				oldcount = parseInt($("#sum").children("."+oldstatus.toLowerCase()).attr("val"));
				oldcount2 = parseInt($("#sum").children("."+newstate.toLowerCase()).attr("val"));
				$("#sum").children("."+oldstatus.toLowerCase()).attr("val", oldcount-1);
				$("#sum").children("."+newstate.toLowerCase()).attr("val", oldcount2+1);

				// adjust progressbars
				$("#sum").children("."+oldstatus.toLowerCase()).first().children().first().children().first().css("width", ((oldcount-1) / summembers * widthProgressbar)+"px").html(oldcount-1);
				$("#sum").children("."+newstate.toLowerCase()).first().children().first().children().first().css("width", ((oldcount2+1) / summembers * widthProgressbar)+"px").html(oldcount2+1);
				
		}
		}
	});
	
	// handle jobevents
	$.each(json.jobevents , function(i, event) {
		if(event.newstate == "FINISHED" || event.newstate == "FAILED" || event.newstate == "CANCELED") {
			// stop polling
			pollfinished = true;
			
			// add to history
			var jobjson = {};
			jobjson.jobid = json.jobid;
			jobjson.jobname = $("#"+json.jobid).attr("jobname");
			jobjson.time = event.timestamp;
			jobjson.status = event.newstate;
			_fillTableArchive("#jobsArchive", jobjson, true);
			
			// delete table
			$("#"+json.jobid).remove();
			$("#"+json.jobid+"_title").remove();
			$("#"+json.jobid+"_cancel").remove();
			
			// remove from internal list
			for(var i in recentjobs){
			    if(recentjobs[i]==json.jobid){
			    	recentjobs.splice(i,1);
			        break;
			    }
			}
		}
	});

	if(!pollfinished)
		 setTimeout(function() {poll(json.jobid)}, 2000);
	else if(recentjobs.length == 0) {
		// wait long enough for job to be completely removed on server side before new init
		setTimeout(init, 5000);
	}
}

var archive_finished = 0;
var archive_failed = 0;
var archive_canceled = 0;

/*
 * Creates job history table
 */
function fillTableArchive(table, json) {
	$(table).html("");
	
	$("#jobs-finished").html(archive_finished);
	$("#jobs-failed").html(archive_failed);
	$("#jobs-canceled").html(archive_canceled);
	
	$.each(json, function(i, job) {
		_fillTableArchive(table, job, false)
	});
}

/*
 * Adds one row to job history table
 */
function _fillTableArchive(table, job, prepend) {
	
	// no duplicates
	if($("#"+job.jobid+"_archive").length > 0) {
		return false;
	}
	
	if(prepend)
		$(table).prepend(
				"<li id=\""+job.jobid+"_archive\"><a href=\"/analyze.html?job=" + job.jobid + "\">"
						+ job.jobname + " (time: "
						+ formattedTimeFromTimestamp(parseInt(job.time))
						+ ")</a></li>");
	else
		$(table).append(
				"<li><a href=\"/analyze.html?job=" + job.jobid + "\">"
						+ job.jobname + " (time: "
						+ formattedTimeFromTimestamp(parseInt(job.time))
						+ ")</a></li>");
	if (job.status == "FINISHED")
		archive_finished++;
	if (job.status == "FAILED")
		archive_failed++;
	if (job.status == "CANCELED")
		archive_canceled++;
	
	$("#jobs-finished").html(archive_finished);
	$("#jobs-failed").html(archive_failed);
	$("#jobs-canceled").html(archive_canceled);
}