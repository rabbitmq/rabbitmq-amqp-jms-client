#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
ci/build-documentation.sh

version=$(cat docs/target/library-version.txt)
case "$version" in
  *-SNAPSHOT) channel=snapshot ;;
  *-*) channel=milestone ;;
  *) channel=stable ;;
esac

source_revision=$(git rev-parse --short HEAD)
temporary_dir=$(mktemp -d)
site="$temporary_dir/site"
temporary_branch="docs-publish-${temporary_dir##*/}"
cleanup() {
  if [[ -d "$site" ]]; then
    git worktree remove --force "$site"
  fi
  if git show-ref --verify --quiet "refs/heads/$temporary_branch"; then
    git branch -D "$temporary_branch"
  fi
  rmdir "$temporary_dir"
}
trap cleanup EXIT

pages_ref=$(git ls-remote --heads origin gh-pages)
if [[ -n "$pages_ref" ]]; then
  git fetch origin gh-pages
  git worktree add --detach "$site" FETCH_HEAD
else
  git worktree add --orphan -b "$temporary_branch" "$site"
fi

mkdir -p "$site/$version/htmlsingle" "$site/2.x/$channel/htmlsingle"
cp docs/target/htmlsingle/index.html "$site/$version/htmlsingle/index.html"
cp docs/target/htmlsingle/index.html "$site/2.x/$channel/htmlsingle/index.html"
touch "$site/.nojekyll"

{
  cat <<'HTML'
<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RabbitMQ AMQP JMS Client Documentation</title>
<h1>RabbitMQ AMQP JMS Client Documentation</h1>
<p>2.x: Jakarta Messaging 3.1 (jakarta.jms), Java 11 or later.</p>
<ul>
HTML
  for entry in stable milestone snapshot; do
    if [[ -f "$site/2.x/$entry/htmlsingle/index.html" ]]; then
      printf '<li><a href="2.x/%s/htmlsingle/index.html">2.x %s</a></li>\n' "$entry" "$entry"
    fi
  done
  printf '</ul>\n</html>\n'
} > "$site/index.html"

git -C "$site" add .
if git -C "$site" diff --cached --quiet; then
  echo "Documentation is already up to date"
  exit 0
fi
git -C "$site" commit -m "Publish documentation for $version ($source_revision)"
git -C "$site" push origin HEAD:gh-pages
