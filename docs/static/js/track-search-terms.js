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

'use strict';

function debounce(fn, wait) {
  var timeout
  return function() {
    clearTimeout(timeout)
    var args = arguments;
    timeout = setTimeout(function() {
      fn.apply(this, args)
    }, (wait || 1))
  }
}

// Function which is called to track the actual search term
function trackSearchQuery(e){
  const searchHits = window.bookSearchIndex.search(input.value, 10);
  //Call Matomo's trackSiteSearch function to track the search query in its internal search tracking capability
  _paq.push(['trackSiteSearch',
      // Search keyword searched for
      input.value,
      // Search category selected in your search engine. If you do not need this, set to false
      false,
      // Number of results on the Search results page. Zero indicates a 'No Result Search Keyword'. Set to false if you don't know
      searchHits.length
  ]);
}

// Whenever the search tracking event listener is fired, use the debounce function to track if we should measure the search term
// We assume that if the value of the search term hasn't changed in 2 seconds, the user has searched for this specific term
var debouncedSearchQuery = debounce(trackSearchQuery, 2000);

/* Add event listener to track what users are searching for on the site */
var input = document.querySelector('#book-search-input');
input.addEventListener('input', debouncedSearchQuery);
