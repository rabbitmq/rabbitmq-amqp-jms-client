#!/usr/bin/env bash
#
# Releases the version defined in release-versions.txt:
#   - sets the revision property to the release version, commits, tags (v<version>)
#   - deploys to Maven Central (deployment to publish manually in the Central Portal)
#   - sets the revision property to the next development version, commits
#   - pushes the branch and the tag
#
# Tags use a "v" prefix to avoid clashes with upstream Apache Qpid JMS tags.

set -euo pipefail

source ./release-versions.txt
git checkout "$RELEASE_BRANCH"

TAG="v$RELEASE_VERSION"
if git rev-parse -q --verify "refs/tags/$TAG" > /dev/null; then
  echo "error: tag $TAG already exists" >&2
  exit 1
fi

set_revision() {
  perl -pi -e "s{<revision>[^<]*</revision>}{<revision>$1</revision>}" pom.xml
  grep -q "<revision>$1</revision>" pom.xml
}

set_revision "$RELEASE_VERSION"
git commit -a -m "Release $RELEASE_VERSION"
git tag -a "$TAG" -m "Release $RELEASE_VERSION"

if [[ $RELEASE_VERSION == *[RCM]* ]]
then
  MAVEN_PROFILE="milestone"
  PRERELEASE="true"
else
  MAVEN_PROFILE="release"
  PRERELEASE="false"
fi
if [[ -n "${GITHUB_ENV:-}" ]]; then
  echo "prerelease=$PRERELEASE" >> "$GITHUB_ENV"
fi

./mvnw clean deploy -P "$MAVEN_PROFILE" -DskipTests --no-transfer-progress

set_revision "$DEVELOPMENT_VERSION"
git commit -a -m "Set version to $DEVELOPMENT_VERSION"

git push origin "$RELEASE_BRANCH" "$TAG"
