/** 
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*   http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

/**
 * Function to synchronize all tabs on a page to a given user selection. 
 * 
 * The selection of a tab group should only change if it contains an 
 * input with the given tabId. Otherwise, its current seletion should
 * remain unchanged.
 * 
 * See layouts/shortcodes/tabs.html
 */
function onSwitch(tabId) {
    var selectorForId = "[data-tab-group='flink-tabs'][data-tab-item='" + tabId + "']";

    Array
        // find all tab group elements on the page
        .from(document.getElementsByClassName("book-tabs"))
        // filter out any elements that do not contain 
        // the specific tab the user wants to switch to.
        // these tabs should remain on their current selection
        .filter(div => div.querySelectorAll(selectorForId).length > 0)
        // extract the input elements for all tab groups
        // that do contain the target tab id
        .flatMap(div => Array.from(div.querySelectorAll("[data-tab-group='flink-tabs']")))
        // check input elements that contain the selected tabId
        // and uncheck all others
        .forEach(input => {
            if (input.matches(selectorForId)) {
                input.setAttribute("checked", "checked")
            } else {
                input.removeAttribute("checked")
            }
        });
}

/**
 * Function to collapse the ToC in desktop mode.
 */
function collapseToc() {
    document.querySelector(".book-toc").style["display"] = "none";
    document.querySelector(".expand-toc").style["display"] = "block";

    sessionStorage.setItem("collapse-toc", "true");
}

/**
 * Function to expand the ToC in desktop mode.
 */
function expandToc() {
    document.querySelector(".book-toc").style["display"] = "block";
    document.querySelector(".expand-toc").style["display"] = "none";

    sessionStorage.removeItem("collapse-toc");
}



/**
 * Selects all text within the given container and copies it
 * to the users clipboard. If any actions are not supported
 * by a users browser this function will do nothing.
 */
function selectTextAndCopy(containerId) {
    if (wasLastCopied(containerId)) {
        return;
    }

    try {
        if (highlightContent(containerId)) {
            if (document.queryCommandSupported("copy")) {
                document.execCommand("copy") && showCurrentCopyAlert(containerId);
            }
        }  
    } catch (e) {}
}

/**
 * Checks if this container was the most recent one copied
 * to the clipboard. This was users can double click and
 * highlight specific portions of the dep. 
 */
function wasLastCopied(containerId) {
    return document
        .querySelector("[copyable='flink-module'][copyattribute='" + containerId + "'")
        .style["display"] == "block";
}

/**
 * Highlights the content of the given container.
 * Returns true on success, false otherwise.
 */
function highlightContent(containerId) {
    try {
        if (document.selection) {
            var range = document.body.createTextRange();
            range.moveToElementText(document.getElementById(containerId));
            range.select().createTextRange();
            return true;
        } else if (window.getSelection) {
            var range = document.createRange();
            range.selectNode(document.getElementById(containerId));
            window.getSelection().removeAllRanges();
            window.getSelection().addRange(range);
            return true;
        } else {
            return false;
        }
    } catch (err) {
        // Text highlighting is not supported by this browser
        return false;
    }
}


/**
 * Makes the copy alert for the given container
 * visible while hiding all others.
 */
function showCurrentCopyAlert(containerId) {
    document
        .querySelectorAll("[copyable='flink-module']")
        .forEach(function (alert) {
            alert.style["display"] = "none";
        });

    var alert = document.querySelector("[copyable='flink-module'][copyattribute='" + containerId + "'");

    alert.style["text-align"] = "center";
    alert.style["display"] = "block";
}

/**
 * Adds forEach to NodeList for old versions
 * of microsoft IE and Edge.
 */
if (window.NodeList && !NodeList.prototype.forEach) {
    NodeList.prototype.forEach = Array.prototype.forEach;
}

/**
 * Adds forEach to Element for old versions
 * of microsoft IE and Edge.
 */
if (!Element.prototype.matches) {
  Element.prototype.matches = Element.prototype.msMatchesSelector ||
                              Element.prototype.webkitMatchesSelector;
}


document.addEventListener("DOMContentLoaded", function(event) { 
    if (sessionStorage.getItem("collapse-toc") === "true") {
        collapseToc();
    }

    // Display anchor links when hovering over headers. For documentation of the
    // configuration options, see the AnchorJS documentation.
    anchors.options = {
        placement: 'right'
    };
    anchors.add();
});