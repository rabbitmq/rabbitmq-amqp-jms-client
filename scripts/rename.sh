#!/usr/bin/env bash
#
# Transforms a pristine Apache Qpid JMS tree into the RabbitMQ AMQP JMS client tree:
#   - Java packages:      org.apache.qpid.jms      -> com.rabbitmq.client.jms
#   - Maven coordinates:  org.apache.qpid:qpid-jms-* -> com.rabbitmq.client:jms-client-*
#   - Module directories: qpid-jms-*               -> jms-client-*
#   - Project version:    upstream version         -> ${revision} (value defined by the fork)
#
# Lines changed by upstream releases are normalized (project version, SCM tag),
# so upstream version bumps never conflict with the fork's own versions.
#
# Must be run at the root of a git working tree containing an *unmodified* upstream
# tree (e.g. created with "git read-tree -u --reset <upstream-ref>").
# The output must stay deterministic: the upstream-renamed branch relies on it.
# Dependencies from the org.apache.qpid group (e.g. proton-j) are left untouched.

set -euo pipefail

if [[ ! -f pom.xml || ! -d qpid-jms-client ]]; then
  echo "error: run this script at the root of a pristine upstream qpid-jms tree" >&2
  exit 1
fi

# 1. File contents (tracked text files containing something to rename)
git grep -lIz -E 'org[./]apache[./]qpid|qpid-jms' -- . | xargs -0 perl -0777 -pi -e '
  s{<Bundle-SymbolicName>org\.apache\.qpid\.jms\.client</Bundle-SymbolicName>}{<Bundle-SymbolicName>com.rabbitmq.client.jms</Bundle-SymbolicName>}g;
  s{\borg\.apache\.qpid\.jms\b}{com.rabbitmq.client.jms}g;
  s{\borg/apache/qpid/jms\b}{com/rabbitmq/client/jms}g;
  s{<groupId>org\.apache\.qpid</groupId>(\s*<artifactId>(?:qpid-jms-|apache-qpid-jms))}{<groupId>com.rabbitmq.client</groupId>$1}g;
  s{\borg\.apache\.qpid:((?:apache-)?qpid-jms)}{com.rabbitmq.client:$1}g;
  s{\bapache-qpid-jms\b}{jms-client-dist}g;
  s{\bqpid-jms-client\b}{jms-client}g;
  s{\bqpid-jms-(discovery|docs|examples|interop-tests|activemq-tests|parent)\b}{jms-client-$1}g;
'

# 2. Release-managed lines in POMs (literal project versions, SCM tag)
git ls-files -z -- pom.xml '*/pom.xml' | xargs -0 perl -0777 -pi -e '
  s{(<artifactId>jms-client(?:-[\w-]+)?</artifactId>\s*<version>)[^<\$]+(</version>)}{$1\${revision}$2}g;
  s{(<scm>.*?<tag>)[^<]*(</tag>)}{$1HEAD$2}s;
'

# 3. Module directories (nested module first)
mv qpid-jms-interop-tests/qpid-jms-activemq-tests qpid-jms-interop-tests/jms-client-activemq-tests
mv qpid-jms-interop-tests jms-client-interop-tests
mv qpid-jms-client jms-client
mv qpid-jms-discovery jms-client-discovery
mv qpid-jms-docs jms-client-docs
mv qpid-jms-examples jms-client-examples
mv apache-qpid-jms jms-client-dist

# 4. Package directories (sources, resources, META-INF/services)
find . -path ./.git -prune -o -path '*/target' -prune -o \
  -type d -path '*/org/apache/qpid/jms' -print | sort | while read -r dir; do
  base="${dir%/org/apache/qpid/jms}"
  mkdir -p "$base/com/rabbitmq/client"
  mv "$dir" "$base/com/rabbitmq/client/jms"
  rmdir "$base/org/apache/qpid" "$base/org/apache" "$base/org" 2>/dev/null || true
done

# 5. Sanity check
leftovers=$(git grep -lI --untracked -E 'org[./]apache[./]qpid[./]jms|\bqpid-jms-(client|discovery|docs|examples|interop-tests|activemq-tests|parent)\b' -- . || true)
if [[ -n "$leftovers" ]]; then
  echo "warning: old names still present in:" >&2
  echo "$leftovers" >&2
  exit 2
fi
poms=$(find . -path ./.git -prune -o -path '*/target' -prune -o -name pom.xml -print | wc -l)
versioned=$(find . -path ./.git -prune -o -path '*/target' -prune -o -name pom.xml -print0 \
  | xargs -0 grep -l -F '<version>${revision}</version>' | wc -l)
if [[ $poms -ne $versioned ]]; then
  echo "warning: \${revision} set in $versioned POM(s) out of $poms" >&2
  exit 2
fi
echo "rename done"
