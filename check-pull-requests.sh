#!/bin/bash

. "$spellchecker/common.sh"
if [ $(uname) = "Linux" ]; then
  date_to_epoch() {
    date -u -d "$1" +'%s'
  }
else
  date_to_epoch() {
    date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$1" +'%s'
  }
fi
timeframe=${timeframe:-60}
time_limit=$(( $timeframe * 60 ))
strip_quotes() {
  tr '"' ' '
}

begin_group 'Retrieving open pull requests'
pulls=$temp/pulls.json
escaped=$temp/escaped.b64
pull=$temp/pull.json
fake_event=$temp/fake_event.json
tree_config="$bucket/$project/"
stored_config=$temp/config/
headers=$temp/headers

Q='"'

owner=${GITHUB_REPOSITORY%/*}
repo=${GITHUB_REPOSITORY#*/}
length=20

get_open_pulls() {
query=$(echo "query {
  repository(owner:${Q}$owner${Q} name:${Q}$repo${Q}) {
    pullRequests(first: $length states: OPEN $continue) {
      totalCount
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        baseRefName
        baseRefOid
        baseRepository {
          nameWithOwner
        }
        headRefName
        headRefOid
        headRepository {
          nameWithOwner
          url
        }
        potentialMergeCommit {
          oid
        }
        createdAt
        url
        commits(last: 1) {
          nodes {
            commit {
              pushedDate
            }
          }
        }
      }
    }
  }
}")
echo '{}' | jq --arg query "$query" '.query = $query'
}

if [ -e "$pulls.nodes" ]; then
  echo using cached $pulls.nodes
else
  rm -f "$pulls.nodes"
  touch "$pulls.nodes"
  while :
  do
    curl -s -H "Authorization: token $GITHUB_TOKEN" --header "Content-Type: application/json" --data-binary "$(get_open_pulls)" $GITHUB_GRAPHQL_URL > $pulls
    cat "$pulls" | jq .data.repository.pullRequests > "$pulls.pull_requests"
    cat "$pulls.pull_requests" | jq -c '.nodes[]' >> "$pulls.nodes"
    cat "$pulls.pull_requests" | jq .pageInfo > "$pulls.page_info"
    if [ "$(cat "$pulls.page_info" | jq -c -r .hasNextPage)" != "true" ]; then
      continue=''
      break
    fi
    continue="after: ${Q}$(cat "$pulls.page_info"|jq -c -r .endCursor)${Q}"
  done
fi

cat "$pulls.nodes" |jq -c -r '{
 head_repo: .headRepository.nameWithOwner,
 base_repo: .baseRepository.nameWithOwner,
 head_ref: .headRefName,
 head_sha: .headRefOid,
 base_sha: .baseRefOid,
 clone_url: .headRepository.url,
 merge_commit_sha: .potentialMergeCommit.oid,
 created_at: .createdAt,
 updated_at: .commits.nodes[0].commit.pushedDate,
 issue_url: .url | sub("github.com"; "api.github.com/repos") | sub("/pull/(?<id>[0-9]+)$"; "/issues/\(.id)"),
 commits_url: .url | sub("github.com"; "api.github.com/repos") | sub("/pull/(?<id>[0-9]+)$"; "/pulls/\(.id)/commits"),
 comments_url: .url | sub("github.com"; "api.github.com/repos") | sub("/pull/(?<id>[0-9]+)$"; "/issues/\(.id)/comments"),
 } | @base64' > "$escaped"
if [ -s "$escaped" ]; then
  if [ -d "$tree_config" ]; then
    mkdir -p "$stored_config"
    rsync -a "$tree_config" "$stored_config"
  fi
fi
end_group

for a in $(cat "$escaped"); do
  echo "$a" | base64 --decode | jq -r . > $pull
  url=$(cat $pull | jq -r .commits_url | perl -pne 's{://api.github.com/repos/(.*/pull)s}{://github.com/$1}')
  issue_url=$(cat $pull | jq -r .issue_url)
  begin_group "Considering $url"
  created_at=$(cat $pull | jq -r '.updated_at // .created_at')
  created_at=$(date_to_epoch $created_at)
  age=$(( $start / 1000000000 - $created_at ))
  if [ $age -gt $time_limit ]; then
    end_group
    continue
  fi
  head_repo=$(cat $pull | jq -r .head_repo)
  base_repo=$(cat $pull | jq -r .base_repo)
  if [ "$head_repo" = "$base_repo" ]; then
    end_group
    continue
  fi
  head_sha=$(cat $pull | jq -r .head_sha)
  base_sha=$(cat $pull | jq -r .base_sha)
  merge_commit_sha=$(cat $pull | jq -r .merge_commit_sha)
  comments_url=$(cat $pull | jq -r .comments_url)
  commits_url=$(cat $pull | jq -r .commits_url)
  clone_url=$(cat $pull | jq -r .clone_url)
  clone_url=$(echo "$clone_url" | sed -e 's/https/http/')
  head_ref=$(cat $pull | jq -r .head_ref)
  echo "do work for $head_repo -> $base_repo: $head_sha as $merge_commit_sha"
  export GITHUB_SHA="$head_sha"
  export GITHUB_EVENT_NAME=pull_request
  echo '{}' | jq \
    --arg head_sha "$head_sha" \
    --arg base_sha "$base_sha" \
    --arg comments_url "$comments_url" \
    --arg commits_url "$commits_url" \
    -r '{pull_request: {base: {sha: $base_sha}, head: {sha: $head_sha}, comments_url: $comments_url, commits_url: $commits_url }}' \
    > "$fake_event"
  export GITHUB_EVENT_PATH="$fake_event"
  git remote rm pr 2>/dev/null || true
  git remote add pr $clone_url
  cat .git/config
  git fetch pr $head_ref
  git checkout $head_sha
  git remote rm pr 2>/dev/null || true
  end_group
  (
    temp=$(mktemp -d)
    if [ -d "$stored_config" ] && [ ! -d "$tree_config" ]; then
      mkdir -p "$tree_config"
      rsync -a "$stored_config" "$tree_config"
      cleanup_tree_config=1
    fi
    "$spellchecker/unknown-words.sh" || true
    rm -rf "$temp"
    if [ -n "$cleanup_tree_config" ]; then
      rm -rf "$tree_config"
    fi
  )
done
