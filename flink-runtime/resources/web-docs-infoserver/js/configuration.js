
$(document).ready(function() {
	$.ajax({ url : "setupInfo?get=globalC", type : "GET", cache: false, success : function(json) {
		loadConfigTable(json);
	}, dataType : "json",
	});
});

/*
 * Initializes global config table
 */
function loadConfigTable(json) {
	$("#confTable").empty();
	var table = "<table class=\"table table-bordered table-hover table-striped\">";
	table += "<tr><th>Property</th><th>Value</th></tr>";
	for (var key in json) {
		if (json.hasOwnProperty(key)) {
			table += "<tr><td>"+key+"</td><td>"+json[key]+"</td></tr>";
		}
	}
	table += "</table>";
	$("#confTable").append(table);
}
