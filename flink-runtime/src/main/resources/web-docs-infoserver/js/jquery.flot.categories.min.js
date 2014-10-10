/* Flot plugin for plotting textual data or categories.

Copyright (c) 2007-2013 IOLA and Ole Laursen.
Licensed under the MIT license.

Consider a dataset like [["February", 34], ["March", 20], ...]. This plugin
allows you to plot such a dataset directly.

To enable it, you must specify mode: "categories" on the axis with the textual
labels, e.g.

	$.plot("#placeholder", data, { xaxis: { mode: "categories" } });

By default, the labels are ordered as they are met in the data series. If you
need a different ordering, you can specify "categories" on the axis options
and list the categories there:

	xaxis: {
		mode: "categories",
		categories: ["February", "March", "April"]
	}

If you need to customize the distances between the categories, you can specify
"categories" as an object mapping labels to values

	xaxis: {
		mode: "categories",
		categories: { "February": 1, "March": 3, "April": 4 }
	}

If you don't specify all categories, the remaining categories will be numbered
from the max value plus 1 (with a spacing of 1 between each).

Internally, the plugin works by transforming the input data through an auto-
generated mapping where the first category becomes 0, the second 1, etc.
Hence, a point like ["February", 34] becomes [0, 34] internally in Flot (this
is visible in hover and click events that return numbers rather than the
category labels). The plugin also overrides the tick generator to spit out the
categories as ticks instead of the values.

If you need to map a value back to its label, the mapping is always accessible
as "categories" on the axis object, e.g. plot.getAxes().xaxis.categories.

*/(function(e){function n(e,t,n,r){var i=t.xaxis.options.mode=="categories",s=t.yaxis.options.mode=="categories";if(!i&&!s)return;var o=r.format;if(!o){var u=t;o=[],o.push({x:!0,number:!0,required:!0}),o.push({y:!0,number:!0,required:!0});if(u.bars.show||u.lines.show&&u.lines.fill){var a=!!(u.bars.show&&u.bars.zero||u.lines.show&&u.lines.zero);o.push({y:!0,number:!0,required:!1,defaultValue:0,autoscale:a}),u.bars.horizontal&&(delete o[o.length-1].y,o[o.length-1].x=!0)}r.format=o}for(var f=0;f<o.length;++f)o[f].x&&i&&(o[f].number=!1),o[f].y&&s&&(o[f].number=!1)}function r(e){var t=-1;for(var n in e)e[n]>t&&(t=e[n]);return t+1}function i(e){var t=[];for(var n in e.categories){var r=e.categories[n];r>=e.min&&r<=e.max&&t.push([r,n])}return t.sort(function(e,t){return e[0]-t[0]}),t}function s(t,n,r){if(t[n].options.mode!="categories")return;if(!t[n].categories){var s={},u=t[n].options.categories||{};if(e.isArray(u))for(var a=0;a<u.length;++a)s[u[a]]=a;else for(var f in u)s[f]=u[f];t[n].categories=s}t[n].options.ticks||(t[n].options.ticks=i),o(r,n,t[n].categories)}function o(e,t,n){var i=e.points,s=e.pointsize,o=e.format,u=t.charAt(0),a=r(n);for(var f=0;f<i.length;f+=s){if(i[f]==null)continue;for(var l=0;l<s;++l){var c=i[f+l];if(c==null||!o[l][u])continue;c in n||(n[c]=a,++a),i[f+l]=n[c]}}}function u(e,t,n){s(t,"xaxis",n),s(t,"yaxis",n)}function a(e){e.hooks.processRawData.push(n),e.hooks.processDatapoints.push(u)}var t={xaxis:{categories:null},yaxis:{categories:null}};e.plot.plugins.push({init:a,options:t,name:"categories",version:"1.0"})})(jQuery);