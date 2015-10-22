/*!
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

var maxColumnWidth = 200;
var minColumnWidth = 100;

// global variable for the currently requested plan
var pactPlanRequested = 0;

/*
 * This function toggels the child checkbox on and of, depending on the parent's state
 */
function toggleShowPlanBox(box)
{
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
  $('#upload_error_text').fadeOut("fast", function () { $('#upload_error_text')[0].innerHTML = "" + message;
                                                           $('#upload_error_text').fadeIn("slow"); } );
}

/*
 * Checks the selected file and triggers an upload, if all is correct.
 */
function processUpload()
{

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

  if (box.is(':checked')) {
    $('.jobItemCheckbox').attr('checked', false);
    box.attr('checked', true);
    var id = box.parentsUntil('.JobListItems').parent().attr('id').substr(4);
    var assemblerClass = box.attr('id');

    $('#mainCanvas').html('');
    pactPlanRequested = id;

    $.ajax({
        type: "GET",
        url: "pactPlan",
        data: { job: id, assemblerClass: assemblerClass},
        success: function(response) { showPreviewPlan(response); }
    });
  }
  else {
    $('#mainCanvas').html('');
  }
}

/*
 * Function that takes the returned plan and draws it.
 */
function showPreviewPlan(data)
{
	$("#mainCanvas").empty();
    var svgElement = "<div id=\"attach\"><svg id=\"svg-main\" width=500 height=500><g transform=\"translate(20, 20)\"/></svg></div>";
    $("#mainCanvas").append(svgElement);
    drawGraph(data.plan, "#svg-main");
    pactPlanRequested = 0;
    
    //activate zoom buttons
    activateZoomButtons();
//  }
}

/*
 * Asynchronously loads the jobs and creates a list of them.
 */
function loadJobList()
{
  $.get("jobs", { action: "list" }, createJobList);
}

/*
 * Triggers an AJAX request to delete a job.
 */
function deleteJob(id)
{
  var name = id.substr(4);
  $.get("jobs", { action: "delete", filename: name }, loadJobList);
}

/*
 * Creates and lists the returned jobs.
 */
function createJobList(data)
{
  var markup = "";
   
  var lines = data.split("\n");
  for (var i = 0; i < lines.length; i++)
  {
    if (lines[i] == null || lines[i].length == 0) {
      continue;
    }
    
    var date = "unknown date";
    var assemblerClass = "<em>no entry class specified</em>";
    
    var tokens = lines[i].split("\t");
    var name = tokens[0];
    if (tokens.length > 1) {
      date = tokens[1];
      if (tokens.length > 2) {
        assemblerClass = tokens[2];
      }
    }
    
    var entries = assemblerClass.split("#");
    var classes = entries[0].split(",");
    
    markup += '<div id="job_' + name + '" class="JobListItems"><table class="table"><tr>';
    markup += '<td colspan="2"><p class="JobListItemsName">' + name + '</p></td>';
    markup += '<td><p class="JobListItemsDate">' + date + '</p></td>';
    markup += '<td width="30px"><img class="jobItemDeleteIcon" src="img/delete-icon.png" width="24" height="24" /></td></tr>';
    
    var j = 0;
    for (var idx in classes) {
      markup += '<tr><td width="30px;"><input id="' + classes[idx] + '" class="jobItemCheckbox" type="checkbox"></td>';
      markup += '<td colspan="3"><p class="JobListItemsDate" title="' + entries[++j] + '">' + classes[idx] + '</p></td></tr>';
    }
    markup += '</table></div>';
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
   var job = $('.jobItemCheckbox:checked');
   if (job.length == 0) {
     $('#run_error_text').fadeOut("fast", function () { $('#run_error_text')[0].innerHTML = "Select a job to run.";
                                                           $('#run_error_text').fadeIn("slow"); } );
     return;
   }
   
   var jobName = job.parentsUntil('.JobListItems').parent().attr('id').substr(4);
   var assemblerClass = job.attr('id');
   
   var showPlan = $('#showPlanCheck').is(':checked');
   var suspendPlan = $('#suspendJobDuringPlanCheck').is(':checked');
   var options = $('#commandLineOptionsField').attr('value'); //TODO? Replace with .val() ?
   var args = $('#commandLineArgsField').attr('value'); //TODO? Replace with .val() ?
   
   var url;
   if (assemblerClass == "<em>no entry class specified</em>") {
      url = "runJob?" + $.param({ action: "submit", options: options, job: jobName, arguments: args, show_plan: showPlan, suspend: suspendPlan});
   } else {
      url = "runJob?" + $.param({ action: "submit", options: options, job: jobName, assemblerClass: assemblerClass, arguments: args, show_plan: showPlan, suspend: suspendPlan});
   }
   
   window.location = url;
}

/*
 * Document initialization.
 */
$(document).ready(function ()
{
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
