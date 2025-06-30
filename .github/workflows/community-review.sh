#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# Community review GitHub Action - sets the community review labels on PRs to show case
# community review activity. See Flip-518 for details
set -e

# =============================================================================
# Global variables
# =============================================================================
REPO_OWNER=apache
REPO_NAME=flink
LGTM_LABEL="community-reviewed-LGTM"
COMMUNITY_REVIEW_LABEL="community-reviewed"
USER_CACHE_FILENAME="user_cache.txt"

# =============================================================================
# Community review script - is passed a github token that it uses for authentication
# -  gets all of the open PRs from the Flink Github repo. As the API calls have a limit,
# extra calls will be made to get the rest of the PR pages, and review as required.
# - For each PR the reviews state and whether the reviewers is the committer decides whether
# or not a label is applied to PR. The 2 labels that this script can add are:
#    - community-reviewed-LGTM - set if there have been 2 approves by non-committers, no committer reviews and
#      no changes requested
#    - community-reviewed - set if a non-committer has reviewed the PR.
#  Note only one of the above labels should be present on a PR.
# =============================================================================
main() {
  local token="${1?missing token}"
  local GET_PRS_TEMPLATE='{
  "query":
    "query {
      repository(owner: \"<<REPO_OWNER>>\" name: \"<<REPO_NAME>>\") {
        pullRequests(first:100, <<AFTER_CURSOR>> states: [OPEN]) {
          edges {
            node {
              number
              isDraft
              timelineItems(first: 100, itemTypes: [PULL_REQUEST_REVIEW]) {
                nodes {
                  ... on PullRequestReview {
                    author {
                      login
                    }
                    state
                    createdAt
                 }
                }
                pageInfo {
                  endCursor
                  hasNextPage
                }
              }
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }"
  }'
  # prepare payload template
  local payloadTemplate
  payloadTemplate="$(echo "$GET_PRS_TEMPLATE" | tr -d '\n')"  # the query should be a one-liner, without newlines
  payloadTemplate="$(replace_template_value "$payloadTemplate" "REPO_OWNER" "${REPO_OWNER}")"
  payloadTemplate="$(replace_template_value "$payloadTemplate" "REPO_NAME" "${REPO_NAME}")"
  local pullRequests="[]"
  local hasNextPage=true
  local cursor=""
  local payload
  
  echo "=== Community review GitHub Action ==="
  while [ "$hasNextPage" = "true" ]
  do
      if [[ -n $cursor ]]; then
        # we have a cursor - so need to page
        payload="$(replace_template_value "$payloadTemplate" 'AFTER_CURSOR' "after: \\\\\"$cursor\\\\\",")"
      else
        # no cursor so no need to page
        payload="$(replace_template_value "$payloadTemplate" 'AFTER_CURSOR' '')"
      fi
      restResponse="$(call_github_graphql_api "$token" "$payload")"
      check_github_graphql_response "$restResponse"
      receivedPullRequests="$(jq '.data.repository.pullRequests.edges' <<< "$restResponse")"    
      
      printf "Filtering %4s received pull requests... "  "$(JSONArrayLength "$receivedPullRequests")"
      receivedPullRequests=$(jq '[.[] | select((.node.isDraft = false) and (.node.timelineItems.nodes | type != "array" or length > 0))]' <<< "$receivedPullRequests")
      printf " %2s PR retained" "$(JSONArrayLength "$receivedPullRequests")"
      
      pullRequests=$(jq --argjson a1 "$pullRequests" --argjson a2 "$receivedPullRequests" '$a1 + $a2' <<< '{}')   
      
      hasNextPage=$(jq  '.data.repository.pullRequests.pageInfo.hasNextPage' <<< "$restResponse")
      cursor=$(jq -r '.data.repository.pullRequests.pageInfo.endCursor' <<< "$restResponse")
      printf " | hasNextPage: %-5s | cursor: %s\n" "${hasNextPage}" "${cursor}"
  done

  process_each_pr "${token}" "${pullRequests}" || exit
  echo "Completed."
}

# =============================================================================
# Take the supplied information about the PRs and process each one.
#
# Arguments:
#   $1 - GitHub API token for authentication
#   $2 - pull Requests - a variable containing a json structure for each PR.
#
# Usage:
#     process_each_pr "${token}" "${pullRequests}"
#
# =============================================================================
process_each_pr() {
  local token="${1?missing token}"
  local pullRequests="${2?missing pull requests}"
  local prNumbersAndPaging
  local prCount

  local token="${1?missing token}"

  # get pr numbers list
  prNumbersAndPaging="$(jq -jr '.[] |  .node.number, "-",  .node.timelineItems.pageInfo.hasNextPage,"\n"' <<< "$pullRequests")"
  prCount=$(wc -l <<< "$prNumbersAndPaging" | xargs)
  local counter=1

  # Process each pr separately in a loop
  while IFS= read -r line;
  do
    local pr_number=${line%-*}
    local hasNextPage=${line#*-}
    
    printf "\n(%s/%s) PR %s - " "$counter" "$prCount" "$pr_number"
    
    # find the node for our pr
    local pr_reviews
    if [[ "$hasNextPage" == "false" ]]; then
      all_reviews="$(jq --argjson number "$pr_number" -r '.[] | select(.node.number==$number) | .node.timelineItems.nodes'  <<< "$pullRequests")"
    else
      all_reviews="$(get_all_reviews_for_pr "$token" "$pr_number")" 
    fi
    # leave only the latest reviews per reviewer in a comma separated form
    pr_reviewers="$(jq  '. | sort_by([.author.login, .createdAt]) | reverse | unique_by(.author.login) | .[] | [.author.login, .state, .createdAt] | join(",")' <<< "$all_reviews")"
    
    printf "Reviews %s Reviewers %s\n" "$(JSONArrayLength "$all_reviews")" "$(wc -l <<< "$pr_reviewers" | xargs)"
    
    process_pr_reviews "$token" "$pr_number" "$pr_reviewers" || exit
    ((counter++))
  done <<< "$prNumbersAndPaging" || exit
}

# =============================================================================
# Process pr reviews for a pr
# The pr reviews a line for each review, with the user, creation time and review state
# This function processes the pr reviews for the pr to obtain
#  community approves
#  request for changes
#  committer approves
#  community reviews
#
# If there are 2 or more community approves, no request for changes and no committer approves we set
# label 'community-reviewed-LGTM' on the PR. If not and there has been a community review, label
# 'community-reviewed' is set.
# If one of the labels is set the other is unset.
#
# Arguments:
#   $1 - GitHub API token for authentication
#   $2 - PR number
#   $3 - PR reviews
# =============================================================================
process_pr_reviews() {
  local token="${1?missing token}"
  local pr_number="${2?missing pr number}"
  local pr_reviews="${3?missing pr reviews}"

  local communityApproves=0
  local requestForChanges=0
  local committerApproves=0
  local communityReviews=0
  local push_permission
  # replace spaces with new lines so the loop will work
  pr_reviews=$(echo "$pr_reviews" | tr ' ' '\n')
  # remove unnecessary double quotes
  pr_reviews="${pr_reviews//\"/}"

  while IFS=, read -r user state time
  do
      printf "%-15s | %-20s | %-20s - checking user permissions..." "$user" "$state" "$time"
      push_permission=$(call_github_get_user_push_permission "$token" "$user") || exit
      printf "%s\n" "$push_permission"

      #see if the user has read role

      if [[ "$push_permission" == "true" ]]; then
          if [[ "$state" == "APPROVED" ]]; then
             ((++committerApproves))
          fi
     else
          ((++communityReviews))
          if [[ "$state" == "APPROVED" ]]; then
             ((++communityApproves))
          fi
     fi

     if [[ "$state" == "CHANGES_REQUESTED" ]]; then
        ((++requestForChanges))
     fi
  done <<< "$pr_reviews"
  echo "communityApproves $communityApproves requestForChanges $requestForChanges committerApproves $committerApproves communityReviews $communityReviews"

  local label_to_post=
  local label_to_delete=
  if [[ $communityApproves -ge 2 && $requestForChanges -eq 0 && $committerApproves -eq 0 ]]; then
    label_to_post=$LGTM_LABEL
    label_to_delete=$COMMUNITY_REVIEW_LABEL
  elif [[ $communityReviews -gt 0 ]]; then
    label_to_post=$COMMUNITY_REVIEW_LABEL
    label_to_delete=$LGTM_LABEL
  fi

  if [[ -n "$label_to_post" ]]; then
   call_github_mutate_label_api "$token" "$label_to_delete" "DELETE" "$pr_number" || exit
   call_github_mutate_label_api "$token" "$label_to_post" "POST" "$pr_number" || exit
  fi
}

# =============================================================================
# Get all the reviews for a pr
# The pr reviews a line for each review, with the user, creation time and review state
# This function processes the pr reviews for the pr to obtain
#  community approves
#  request for changes
#  committer approves
#  community reviews
#
# If there are 2 or more community approves, no request for changes and no committer approves we set
# label 'community-reviewed-LGTM' on the PR. If not and there has been a community review, label
# 'community-reviewed' is set.
# If one of the labels is set the other is unset.
#
# Arguments:
#   $1 - GitHub API token for authentication
#   $2 - PR number
# =============================================================================
get_all_reviews_for_pr() {
  local token="${1?missing token}"
  local pr_number="${2?missing pr number}"

  local cursor=""
  local hasNextPage="true"
  local payloadTemplate
  local cutdownRestResponse

  local GET_REVIEWS_TEMPLATE='{
  "query":
    "query {
      repository(owner: \"<<REPO_OWNER>>\" name: \"<<REPO_NAME>>\") {
        pullRequest(number: <<PR_NUMBER>>) {
          id
          number
          timelineItems(first: 100 <<AFTER_CURSOR>> itemTypes: [PULL_REQUEST_REVIEW] ) {
            nodes {
              ... on PullRequestReview {
                author {
                  login
                }
                state
                createdAt
              }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
      }
    }"
  }'

  payloadTemplate=$(echo "$GET_REVIEWS_TEMPLATE" | tr -d '\n')  # the query should be a one-liner, without newlines
  payloadTemplate="$(replace_template_value "$payloadTemplate" "REPO_OWNER" "${REPO_OWNER}")"
  payloadTemplate="$(replace_template_value "$payloadTemplate" "REPO_NAME" "${REPO_NAME}")"
  payloadTemplate="$(replace_template_value "$payloadTemplate" "PR_NUMBER" "${pr_number}")"

  local all_reviews_for_pr=""
  while [[ "$hasNextPage" == "true" ]]
  do
    if [[ -n $cursor ]]; then
       payload="$(replace_template_value "$payloadTemplate" 'AFTER_CURSOR' "after: \\\\\"$cursor\\\\\", ")"
    else
       payload="$(replace_template_value "$payloadTemplate" 'AFTER_CURSOR' '')"
    fi
    restResponse="$(call_github_graphql_api "$token" "$payload")"
    check_github_graphql_response "$restResponse"
    local cutdownRestResponse
    cutdownRestResponse="$(jq '.data.repository.pullRequest.timelineItems.nodes' <<< "$restResponse")"
    hasNextPage=$(jq  '.data.repository.pullRequest.timelineItems.pageInfo.hasNextPage' <<< "$restResponse")
    cursor=$(jq  '.data.repository.pullRequest.timelineItems.pageInfo.endCursor' <<< "$restResponse")
    # remove quotes from cursor
    cursor="${cursor//\"/}"
    # append to all reviews into a single json array
    all_reviews_for_pr=$(echo -e "${all_reviews_for_pr}" "$cutdownRestResponse" | jq '.[]' | jq -s)
  done
  echo "$all_reviews_for_pr"
}

# ======================================
# Utility functions
# ======================================

# =============================================================================
# Send a POST request to the GitHub GraphQL API and output the response.
# Use in combination with the check_github_graphql_response function.
# Arguments:
#   $1 - GitHub API token for authentication
#   $2 - JSON payload to be sent with the request
#   
# Usage:
#    restResponse="$(call_github_graphql_api "$token" "$payload")"
#    check_github_graphql_response "$restResponse"
# =============================================================================
call_github_graphql_api() {
  local token="${1?missing token}"
  local payload="${2?missing payload}"
  
   curl --fail --no-progress-meter \
    -H "Content-Type: application/json" \
    -X POST \
    -H "Authorization: Bearer ${token}" \
    -d "${payload}" \
    "https://api.github.com/graphql"
}

# =============================================================================
# Check the response of the cURL request. Exit in case of error.
# Arguments:
#   $1 - response from callAPI 
# =============================================================================
check_github_graphql_response() {
  local response="${1?missing response}"
  
  # check if response contains a valid JSON
  if jq -e . >/dev/null 2>&1 <<<"$response"; then
    # The cURL request can be successful, but still return an error if the data it receives is
    # incorrect, such as a malformed payload.
    if [[ "$(jq 'has("errors") and .errors != null' <<< "$response")" == "true" ]]; then
      # display the error and terminate
      echo "ERROR received: $response"; exit 1;
    fi
  else
    # The cURL request failed and received no response. cURL already displayed the error.
    exit 1
  fi
}

# =============================================================================
# Replaces a placeholder string <<PLACEHOLDER>> in a template with a specified value in a template.
# Expected arguments 
#   $1 - The template string.
#   $2 - The placeholder string to replace.
#   $3 - The value to substitute in place of the template string.
# =============================================================================
replace_template_value() {
  local text=${1?missing text to update}
  local templateName=${2?missing template name}
  local value=${3?missing value}
  echo "$text" | sed -r "s/<<${templateName}>>/${value}/"
}

# =============================================================================
# Count number of item of a JSON array
# Expected arguments 
# =============================================================================
JSONArrayLength() {
  local jsonArray=${1?missing array}
  echo "${jsonArray}" | jq 'length'
}

# =============================================================================
# Add or delete a label on a Flink PR
# Expected arguments 
#    $1 - GitHub API token for authentication
#    $2 - labelName - name of the label add or delete on the PR
#    $3 - operation POST to add label, DELETE to remove label
#    $4 - pr number on which the label will be added or removed
# 
# Usage: 
#    restResponse="$(call_github_mutate_label_api "$token" "$labelName" "POST" "$pr_number")"
#    check_github_graphql_response "$restResponse"
# =============================================================================
call_github_mutate_label_api() {
  local token="${1?missing token}"
  local labelName="${2?missing label}"
  local operation="${3?missing operation}"
  local prNumber="${4?missing pr number}"

  echo "${operation} label: ${labelName}"
  curl --fail --no-progress-meter -s \
     -H 'Content-Type: application/json' \
     -H "Authorization: bearer $token" \
     -X "$operation" -d "{ \"labels\":[\"${labelName}\"]}" "https://api.github.com/repos/$REPO_OWNER/$REPO_NAME/issues/${prNumber}/labels"
}

# =============================================================================
# Get the push permission of the user - committers have push permission.
# The user permissions are cached in a file. 
# So we only issue the rest call if we have not seen the userName before. 
# Expected arguments 
#    $1 - GitHub API token for authentication
#    $2 - Name of the user to be checked
# 
# Usage: 
#    pushPermission="$(call_github_get_user_push_permission "$token" "$userName")"
# =============================================================================
call_github_get_user_push_permission() {

  local token="${1?missing token}"
  local user_name="${2?missing user}"

  local file_name=$USER_CACHE_FILENAME
  local permissions
  local push_permission
  if [[ -e "$file_name" ]]; then

    while IFS=, read -r user_from_file pushperm_from_file
    do
      if [[ "$user_from_file" = "$user_name" ]]; then
         push_permission=$pushperm_from_file
         break
      fi

    done < $file_name
  fi

  if [[ -z "$push_permission" ]]; then
    # not in the cache so get it from github
    permissions=$(curl --fail --no-progress-meter \
      -H "Accept: application/json" \
      -H "Authorization: Bearer $token"\
      "https://api.github.com/repos/$REPO_OWNER/$REPO_NAME/collaborators/$user_name/permission") || exit
    push_permission=$(jq -r '.user.permissions.push' <<< "$permissions")
    # write line to file
    line="$user_name,$push_permission"
    echo "$line">>$file_name
  fi
  # echo out the permissions
  echo "$push_permission"
}

main "$@"
