var maxColumnWidth = 200;
var minColumnWidth = 100;

// global variable for the currently requested plan
var pactPlanRequested = 0;

/*
 * This function toggels the child checkbox on and of, depending on the parent's state
 */
function toggleShowPlanBox(box)
{
  console.log("toggleShowPlanBox");
  var child = $('#suspendJobDuringPlanCheck');
  
  if (box.is(':checked')) {
    child.attr('disabled', false);
  }
  else {
    child.attr('disabled', true);
  }
}

/*
 * Shows an error message below the upload field.
 */
function showUploadError(message)
{
	  console.log("showUploadError");
  $('#upload_error_text').fadeOut("fast", function () { $('#upload_error_text')[0].innerHTML = "" + message;
                                                           $('#upload_error_text').fadeIn("slow"); } );
}

/*
 * Checks the selected file and triggers an upload, if all is correct.
 */
function processUpload()
{
	  console.log("processUpload");

  var filename = $('#upload_file_input').val();
  var len = filename.length;
  if (len == 0) {
    showUploadError("Please select a file.");
  }
  else if (len > 4 && filename.substr(len - 4, len) == ".jar") {
    $('#upload_form')[0].submit();
  }
  else {
    showUploadError("Please select a .jar file.");
  }
}

/*
 * This function makes sure only one checkbox is selected.
 * Upon selection it initializes the drawing of the pact plan.
 * Upon deselection, it clears the pact plan.
 */
function toggleCheckboxes(box)
{
	  console.log("toggleCheckboxes");

  if (box.is(':checked')) {
    $('.jobItemCheckbox').attr('checked', false);
    box.attr('checked', true);
    var id = box.parentsUntil('.JobListItems').parent().attr('id').substr(4);

    $('#mainCanvas').html('');
    $('#planDescription').html('');
    pactPlanRequested = id;

    $.ajax({
        type: "GET",
        url: "pactPlan",
        data: { job: id },
        success: function(response) { showPreviewPlan(response); }
    });
  }
  else {
    $('#mainCanvas').html('');
    $('#planplanDescription').html('');
  }
}

/*
 * Function that takes the returned plan and draws it.
 */
function showPreviewPlan(data)
{
	console.log("showPreviewPlan");
	//TODO check again the stuff below
//  // check whether this one is still selected
//  var active = $('.jobItemCheckbox:checked');
//  var id = active.parentsUntil('.JobListItems').parent().attr('id').substr(4);
//  
//  if (pactPlanRequested == id) {
//    if (data == undefined || data.jobname == undefined || data.jobname != pactPlanRequested || data.plan == undefined) {
//      pactPlanRequested = 0;
//    }
//
//	if(data.description != undefined) {
//		$('#planDescription').html(data.description);
//	}
	
	$("#mainCanvas").empty();
    var svgElement = "<div id=\"attach\"><svg id=\"svg-main\" width=500 height=500><g transform=\"translate(20, 20)\"/></svg></div>";
    $("#mainCanvas").append(svgElement);
    drawGraph(data.plan, "#svg-main");
    pactPlanRequested = 0;
//  }
}

/*
 * Asynchronously loads the jobs and creates a list of them.
 */
function loadJobList()
{
	console.log("loadJobList");
  $.get("jobs", { action: "list" }, createJobList);
}

/*
 * Triggers an AJAX request to delete a job.
 */
function deleteJob(id)
{
	console.log("deleteJob");
  var name = id.substr(4);
  $.get("jobs", { action: "delete", filename: name }, loadJobList);
}

/*
 * Creates and lists the returned jobs.
 */
function createJobList(data)
{
	console.log("createJobList ");
  var markup = "";
  
  var lines = data.split("\n");
  for (var i = 0; i < lines.length; i++)
  {
    if (lines[i] == null || lines[i].length == 0) {
      continue;

    }
    
    var name = null;
    var date = null;
    var tabIx = lines[i].indexOf("\t");

    if (tabIx > 0) {
      name = lines[i].substr(0, tabIx);
      if (tabIx < lines[i].length - 1) {
        date = lines[i].substr(tabIx + 1);
      }
      else {
        date = "unknown date";
      }
    }
    else {
      name = lines[i];
      date = "unknown date";
    }
    
    
    markup += '<div id="job_' + name + '" class="JobListItems"><table class="table"><tr>';
    markup += '<td width="30px;"><input class="jobItemCheckbox" type="checkbox"></td>';
    markup += '<td><p class="JobListItemsName">' + name + '</p></td>';
    markup += '<td><p class="JobListItemsDate">' + date + '</p></td>';
    markup += '<td width="30px"><img class="jobItemDeleteIcon" src="img/delete-icon.png" width="24" height="24" /></td>';
    markup += '</tr></table></div>';
  }
  
  // add the contents
  $('#jobsContents').html(markup); 
  
  // register the event handler that triggers the delete when the delete icon is clicked
  $('.jobItemDeleteIcon').click(function () { deleteJob($(this).parentsUntil('.JobListItems').parent().attr('id')); } );
  
  // register the event handler, that ensures only one checkbox is active
  $('.jobItemCheckbox').change(function () { toggleCheckboxes($(this)) });
}

/*
 * Function that checks and launches a pact job.
 */
function runJob ()
{
	console.log("runJob");
   var job = $('.jobItemCheckbox:checked');
   if (job.length == 0) {
     $('#run_error_text').fadeOut("fast", function () { $('#run_error_text')[0].innerHTML = "Select a job to run.";
                                                           $('#run_error_text').fadeIn("slow"); } );
     return;
   }
   
   var jobName = job.parentsUntil('.JobListItems').parent().attr('id').substr(4);
   var showPlan = $('#showPlanCheck').is(':checked');
   var suspendPlan = $('#suspendJobDuringPlanCheck').is(':checked');
   var args = $('#commandLineArgsField').attr('value'); //TODO? Replace with .val() ?
   
   var url = "runJob?" + $.param({ action: "submit", job: jobName, arguments: args, show_plan: showPlan, suspend: suspendPlan});
   
   window.location = url;
}

/*
 * Document initialization.
 */
$(document).ready(function ()
{
	console.log("Document ready");
  // hide the error text sections
  $('#upload_error_text').fadeOut("fast");
  $('#run_error_text').fadeOut("fast");
  
  // register the event listener that keeps the hidden file form and the text fied in sync
  $('#upload_file_input').change(function () { $('#upload_file_name_text').val($(this).val()) } );
  
  // register the event handler for the upload button
  $('#upload_submit_button').click(processUpload);
  $('#run_button').click(runJob);
  
  // register the event handler that (in)activates the plan display checkbox
  $('#showPlanCheck').change(function () { toggleShowPlanBox ($(this)); }); 
  
  // start the ajax load of the jobs
  loadJobList();
}); 