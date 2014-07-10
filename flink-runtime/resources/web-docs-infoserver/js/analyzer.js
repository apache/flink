var jsonGlobal;
var widthProgressbar = 120;
 
google.load("visualization", "1");
     
google.setOnLoadCallback(pollArchive);
     
HTTP_GET_VARS=new Array();
  
strGET = document.location.search.substr(1, document.location.search.length);
	
if (strGET != '') {
	gArr = strGET.split('&');
	for (i = 0; i < gArr.length; ++i) {
		v = '';
	vArr = gArr[i].split('=');
		if (vArr.length > 1) {
			v = vArr[1];
		}
		HTTP_GET_VARS[unescape(vArr[0])] = unescape(v);
	}
}

function GET(v) {
	if (!HTTP_GET_VARS[v]) {
		return 'undefined';
	}
	return HTTP_GET_VARS[v];
}

function pollArchive() {
	$.ajax({
		url : "jobsInfo?get=job&job="
				+ GET("job"), type : "GET",
		success : function(json) {
			jsonGlobal = json
			// Fill Table	
			analyzeTime(json, false)
		}, dataType : "json",
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
};

$(document).on("click", "#stack", function() {
	analyzeTime(jsonGlobal, true);
});

$(document).on("click", "#flow", function() {
	analyzeTime(jsonGlobal, false);
});

function analyzeTime(json, stacked) {
	$.each(json, function(i, job) {
		$("#job_timeline").html("");
		$("#time").html(formattedTimeFromTimestamp(job.SCHEDULED));
		$("#run").html(convertTime(job[job.status] - job.SCHEDULED));
		$("#status").html(job.status);
		$("#jobtitle").html(job.jobname);
		
		//create failed table
		if (job.status == "FAILED") {
			failed = "";
			$.each(job.failednodes, function(j, failednode) {
				failed += "<li>" + failednode.node + "<br/>Error: " + failednode.message + "</li>";
			});
			$("#page-wrapper").append("<div class=\"panel panel-primary\"><div class=\"panel-heading\"><h3 class=\"panel-title\">Failed Nodes" +
									 "</h3></div><div id=\"failednodes\" class=\"panel-body\">" +
									 failed +
									 "</div></div>");

		}
		// create accumulators table
		if($.isArray(job.accumulators)  && job.accumulators.length > 0) {
			accuTable = "<div class=\"table-responsive\">" +
					"<table class=\"table table-bordered table-hover table-striped\">" +
					"<tr><td><b>Name</b></td><td><b>Value</b></td></tr>";
			$.each(job.accumulators, function(i, accu) {
				accuTable += "<tr><td>"+accu.name+"</td><td>"+accu.value+"</td></tr>";
			});
			accuTable += "</table></div>";
			$("#accumulators").html(accuTable);
		}

		var data = new google.visualization.DataTable();
		data.addColumn('datetime', 'start');
		data.addColumn('datetime', 'end');
		data.addColumn('string', 'content');
		if (stacked)
			data.addColumn('string', 'group');
		data.addColumn('string', 'className');
		data.addColumn('string', 'groupvertexid');
		var flotdata = [];

		if (stacked)
			data.addRows([
							[
								new Date(job.SCHEDULED),
								,
								"SCHEDULED",
								"9999",
								"scheduled",
								undefined ],
							[
								new Date(job[job.status]),
								,
								job.status,
								"9999",
								"finished",
								undefined ] 
						]);
		else
			data.addRows([
							[
								new Date(job.SCHEDULED),
								,
								"SCHEDULED",
								"scheduled",
								undefined ],
							[
								new Date(job[job.status]),
								,
								job.status,
								"finished",
								undefined ] 
						]);

		var i = job.groupvertices.length;
		
		$.each(job.groupvertices, function(j, groupvertex) {
			// check for reasonable starting time
			if (job.groupverticetimes[groupvertex.groupvertexid].STARTED < 8888888888888) {
				if (stacked) {
					data.addRows([ 
									[
										new Date(job.groupverticetimes[groupvertex.groupvertexid].STARTED),
										new Date(job.groupverticetimes[groupvertex.groupvertexid].ENDED),
										groupvertex.groupvertexname,
										""+ i,
										"running",
										groupvertex.groupvertexid 
									] 
								]);
				} else
					data.addRows([ 
									[
										new Date(job.groupverticetimes[groupvertex.groupvertexid].STARTED),
										new Date(job.groupverticetimes[groupvertex.groupvertexid].ENDED),
										groupvertex.groupvertexname,
										"running",
										groupvertex.groupvertexid 
									] 
								]);
				i--;
			}
		});

		// Instantiate our timeline object.
		var timeline = new links.Timeline(document.getElementById('job_timeline'));

		var onselect = function(event) {
			var row = getSelectedRow(timeline);
			if (row != undefined) {
				if (stacked)
					loadGroupvertex(data.getValue(row, 5));
				else
					loadGroupvertex(data .getValue( row, 4));
			} else {
				//alert("fail");
			}
		};

		// Add event listeners
		google.visualization.events.addListener(timeline, 'select', onselect);

		// Draw our timeline with the created data and options
		timeline.draw(data, {});

	});
}

function loadGroupvertex(groupvertexid) {
	$.ajax({
		url : "jobsInfo?get=groupvertex&job="
				+ GET("job") + "&groupvertex="
				+ groupvertexid, type : "GET",
		success : function(json) {
			//jsonGlobal = json
			// Fill Table	
			analyzeGroupvertexTime(json)
		}, dataType : "json",
	//complete: setTimeout(function() {poll()}, 5000),
	//timeout: 2000
	});
}

function analyzeGroupvertexTime(json) {
	$("#vertices").html("");
	var groupvertex = json.groupvertex;
	$("#vertices").append(
					'<h2>'+ groupvertex.groupvertexname
					+ '</h2><br /><div id="pl_'+groupvertex.groupvertexid+'"></div>');
	
	var data = new google.visualization.DataTable();
	data.addColumn('datetime', 'start');
	data.addColumn('datetime', 'end');
	data.addColumn('string', 'content');
	data.addColumn('string', 'group');
	data.addColumn('string', 'className');
	var cnt = 0;
	$.each(groupvertex.groupmembers, function(k, vertex) {

		data.addRows([
						[
							new Date(json.verticetimes[vertex.vertexid].READY),
							new Date(json.verticetimes[vertex.vertexid].STARTING),
							"ready",
							vertex.vertexinstancename+ "_" + cnt,
							"ready" 
						],
						[
							new Date(json.verticetimes[vertex.vertexid].STARTING),
							new Date(json.verticetimes[vertex.vertexid].RUNNING),
							"starting",
							vertex.vertexinstancename+ "_"+ cnt,
							"starting" 
						] 
					]);

		if (vertex.vertexstatus == "FINISHED")
			data.addRows([
							[
								new Date(json.verticetimes[vertex.vertexid].RUNNING),
								new Date(json.verticetimes[vertex.vertexid].FINISHING),
								" running",
								vertex.vertexinstancename + "_" + cnt,
								"running" 
							],
							[
								new Date(json.verticetimes[vertex.vertexid].FINISHING),
								new Date(json.verticetimes[vertex.vertexid].FINISHED),
								"finishing",
								vertex.vertexinstancename + "_" + cnt,
								"finishing" 
							] 
						]);

		if (vertex.vertexstatus == "CANCELED")
			data.addRows([
							[
								new Date(json.verticetimes[vertex.vertexid].RUNNING),
								new Date(json.verticetimes[vertex.vertexid].CANCELING),
								"running",
								vertex.vertexinstancename + "_" + cnt,
								"running" ],
							[
								new Date(json.verticetimes[vertex.vertexid].CANCELING),
								new Date(	json.verticetimes[vertex.vertexid].CANCELED),
								"canceling",
								vertex.vertexinstancename + "_" + cnt,
								"canceling" 
							] 
						]);

		if (vertex.vertexstatus == "FAILED")
			data.addRows([ 
			               [
							new Date(json.verticetimes[vertex.vertexid].RUNNING),
							new Date(json.verticetimes[vertex.vertexid].FAILED),
							"running - FAILED",
							vertex.vertexinstancename + "_"+ cnt,
							"failed" 
							] 
						]);
		cnt++;
	});

	// Instantiate our timeline object.
	var timeline = new links.Timeline(document.getElementById('pl_' + groupvertex.groupvertexid));

	// Draw our timeline with the created data and options
	timeline.draw(data, {});

}

function getSelectedRow(timeline) {
	var row = undefined;
	var sel = timeline.getSelection();
	if (sel.length) {
		if (sel[0].row != undefined) {
			row = sel[0].row;
		}
	}
	return row;
}