/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Note: This file is originally from the Apache Spark project. */

/* Custom JavaScript code in the MarkDown docs */
function getUrlParameter(sParam) {
  var sPageURL = window.location.search.substring(1),
    sURLVariables = sPageURL.split('&'),
    sParameterName,
    i;

  for (i = 0; i < sURLVariables.length; i++) {
    sParameterName = sURLVariables[i].split('=');
    if (sParameterName[0] === sParam) {
      return sParameterName[1] === undefined ? true : decodeURIComponent(sParameterName[1]);
    }
  }
}

// Enable language-specific code tabs
function codeTabs() {
  var counter = 0;
  var langImages = {
    "scala": "img/scala-sm.png",
    "python": "img/python-sm.png",
    "java": "img/java-sm.png"
  };
  var codeTabParam = null;
  if (typeof window.defaultCodeTab !== "undefined" && window.defaultCodeTab !== "") {
    codeTabParam = window.defaultCodeTab
  }
  codeTabParam = getUrlParameter("code_tab") == null ? codeTabParam : getUrlParameter("code_tab").toLowerCase();
  // Processing duplicated tabs, e.g. if the data-lang="Java/Scala",
  // it will be copied to 2 elements, the "data-lang" value of them would be "Java" and "Scala".
  function splitDataLang() {
    var codeSamples = $(this).children("div");
    codeSamples.each(function() {
      var langData = $(this).data("lang");
      if (langData != null) {
        var langs = langData.split("/");
        if (langs.length > 1) {
          var last = $(this);
          for (let lang of langs) {
            var cloned = $(this).clone();
            cloned.attr("data-lang", lang);
            last.after(cloned);
            last = cloned;
            // process recursively
            $("div.codetabs", cloned).each(splitDataLang);
          }
          $(this).remove();
        }
      }
    });
  }
  $("div.codetabs").each(splitDataLang);
  $("div.codetabs").each(function() {
    $(this).addClass("tab-content");
    var hideTabs = $(this).data("hide-tabs");

    // Insert the tab bar
    var tabBar = $('<ul class="nav nav-tabs" data-tabs="tabs"></ul>');
    $(this).before(tabBar);

    var hasActivedTab = false;
    // Add each code sample to the tab bar:
    codeSamples = $(this).children("div");
    codeSamples.each(function() {
      $(this).addClass("tab-pane");
      var lang = $(this).data("lang");
      var image = $(this).data("image");
      var notabs = $(this).data("notabs");
      var capitalizedLang = lang.substr(0, 1).toUpperCase() + lang.substr(1);
      lang = lang.replace(/[^a-zA-Z0-9]/g, "_");
      // let the first character upper case
      lang = lang.charAt(0).toUpperCase() + lang.slice(1);
      var id = "tab_" + lang + "_" + counter;
      $(this).attr("id", id);
      if (image != null && langImages[lang]) {
        var buttonLabel = "<img src='" +langImages[lang] + "' alt='" + capitalizedLang + "' />";
      } else if (notabs == null) {
        var buttonLabel = "<b>" + capitalizedLang + "</b>";
      } else {
        var buttonLabel = ""
      }
      tabBar.append(
        '<li><a class="tab_' + lang + '" href="#' + id + '">' + buttonLabel + '</a></li>'
      );
      if (codeTabParam === lang.toLowerCase()) {
        $(this).addClass("active");
        tabBar.children("li").last().addClass("active");
        hasActivedTab = true;
      }
    });

    if (!hasActivedTab) {
      codeSamples.first().addClass("active");
      tabBar.children("li").first().addClass("active");
    }
    if (hideTabs != null) {
        tabBar.hide();
    }
    counter++;
  });
  $("ul.nav-tabs a").click(function (e) {
    // Toggling a tab should switch all tabs corresponding to the same language
    // while retaining the scroll position
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $("." + $(this).attr('class')).tab('show');
    $(document).scrollTop($(this).offset().top - scrollOffset);
  });
}

function makeCollapsable(elt, accordionClass, accordionBodyId, title) {
  $(elt).addClass("accordion-inner");
  $(elt).wrap('<div class="accordion ' + accordionClass + '"></div>')
  $(elt).wrap('<div class="accordion-group"></div>')
  $(elt).wrap('<div id="' + accordionBodyId + '" class="accordion-body collapse"></div>')
  $(elt).parent().before(
    '<div class="accordion-heading">' +
      '<a class="accordion-toggle" data-toggle="collapse" href="#' + accordionBodyId + '">' +
             title +
      '</a>' +
    '</div>'
  );
}

// Enable "view solution" sections (for exercises)
function viewSolution() {
  var counter = 0
  $("div.solution").each(function() {
    var id = "solution_" + counter
    makeCollapsable(this, "", id,
      '<i class="icon-ok-sign" style="text-decoration: none; color: #0088cc">' +
      '</i>' + "View Solution");
    counter++;
  });
}

// A script to fix internal hash links because we have an overlapping top bar.
// Based on https://github.com/twitter/bootstrap/issues/193#issuecomment-2281510
function maybeScrollToHash() {
  if (window.location.hash && $(window.location.hash).length) {
    var newTop = $(window.location.hash).offset().top - 57;
    $(window).scrollTop(newTop);
  }
}

$(function() {
  codeTabs();
  viewSolution();

  // Display anchor links when hovering over headers. For documentation of the
  // configuration options, see the AnchorJS documentation.
  anchors.options = {
      placement: 'right'
  };
  anchors.add();

  $(window).bind('hashchange', function() {
    maybeScrollToHash();
  });

  // Scroll now too in case we had opened the page on a hash, but wait a bit because some browsers
  // will try to do *their* initial scroll after running the onReady handler.
  $(window).ready(function() { setTimeout(function() { maybeScrollToHash(); }, 25); }); 
});
