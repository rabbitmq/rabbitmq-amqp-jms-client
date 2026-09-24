#!/usr/bin/env bash
#
# Adds a commit on a renamed-upstream branch with the content of an upstream
# Apache Qpid JMS ref, transformed by rename.sh.
# The branch can then be merged into the corresponding fork branch.
#
# Usage: scripts/sync-upstream.sh <upstream-ref> [<renamed-branch>]
#
#   <upstream-ref>    upstream tag or commit, e.g. 2.12.0, upstream/main, 1.17.0
#   <renamed-branch>  defaults to upstream-renamed (for main),
#                     use upstream-renamed-1.x for the 1.x line
#
# The local <renamed-branch> is first aligned with origin/<renamed-branch>.
# If <renamed-branch> exists neither locally nor on origin, it is created
# from <upstream-ref> (initial fork of a line).
# Expects the "upstream" remote to point to https://github.com/apache/qpid-jms.git
# Neither merges nor pushes anything.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <upstream-ref> [<renamed-branch>]" >&2
  exit 1
fi

REF=$1
BRANCH=${2:-upstream-renamed}
REPO=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
WT="$TMP/sync"
cp "$REPO/scripts/rename.sh" "$TMP/rename.sh"

git fetch upstream --tags
SHA=$(git rev-parse --verify "$REF^{commit}")

# Start from the shared state of the branch (e.g. fresh clone, other maintainer synced)
git fetch origin
if git rev-parse --verify -q "refs/remotes/origin/$BRANCH" > /dev/null; then
  if ! git rev-parse --verify -q "refs/heads/$BRANCH" > /dev/null; then
    git branch --track "$BRANCH" "origin/$BRANCH"
  elif git merge-base --is-ancestor "$BRANCH" "origin/$BRANCH"; then
    git branch -f "$BRANCH" "origin/$BRANCH"
  elif ! git merge-base --is-ancestor "origin/$BRANCH" "$BRANCH"; then
    echo "error: $BRANCH and origin/$BRANCH have diverged, fix this first" >&2
    exit 1
  fi
fi

if git rev-parse --verify -q "refs/heads/$BRANCH" > /dev/null; then
  LAST=$(git log -1 --format='%(trailers:key=Upstream-Commit,valueonly)' "$BRANCH" | tr -d '[:space:]')
  if [[ "$SHA" == "$LAST" ]]; then
    echo "$BRANCH already synced with $REF ($SHA)"
    exit 0
  fi
  git worktree add -q "$WT" "$BRANCH"
else
  LAST=""
  echo "creating $BRANCH from $REF"
  git worktree add -q -b "$BRANCH" "$WT" "$SHA"
fi
trap 'git -C "$REPO" worktree remove --force "$WT"; rm -rf "$TMP"' EXIT

cd "$WT"
git read-tree -u --reset "$SHA"
"$TMP/rename.sh"
git add -A
git commit -q -m "Sync with upstream $REF (renamed)" -m "Upstream-Commit: $SHA"

echo "$BRANCH: $(git log -1 --format='%h %s')"
if [[ -n "$LAST" ]]; then
  echo "upstream changes: git log --oneline $LAST..$SHA"
fi
echo "next step:        merge $BRANCH into the fork branch"
