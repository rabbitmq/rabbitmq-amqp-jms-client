# How this fork works

This repository is a fork of [Apache Qpid JMS](https://github.com/apache/qpid-jms),
published under RabbitMQ's own namespace, with the same license (Apache 2.0).
We rename packages and Maven artifacts, but keep the ability to merge upstream changes.

## Renaming

| | Upstream | Fork |
|---|---|---|
| Root package | `org.apache.qpid.jms` | `com.rabbitmq.client.jms` |
| Maven coordinates | `org.apache.qpid:qpid-jms-client` | `com.rabbitmq.client:jms-client` |
| Other artifacts | `qpid-jms-parent`, `qpid-jms-discovery`, ... | `jms-client-parent`, `jms-client-discovery`, ... |
| Module directories | `qpid-jms-*`, `apache-qpid-jms` | `jms-client-*`, `jms-client-dist` |
| Project version | literal in each POM | `${revision}`, defined once in the root POM |

Only the root package changes: subpackages and class names are the same as upstream.
The `org.apache.qpid.proton` package (proton-j dependency) is not renamed.

The renaming is done by a script, [`scripts/rename.sh`](scripts/rename.sh), never by hand.

## How the fork was created

1. Upstream tag `2.11.0` was transformed with `rename.sh` and committed on a branch
   called `upstream-renamed`.
2. `main` was created from `upstream-renamed`.
3. Fork-specific changes were committed on `main`: own parent POM and metadata, publishing
   to Maven Central, product name, `NOTICE`, `README`, Maven wrapper, scripts.

The `1.x` line was created the same way from upstream tag `1.17.0`
(branch `upstream-renamed-1.x`), and the fork commits were cherry-picked from `main`.

## Branches

```
upstream/main ──(rename.sh)──► upstream-renamed ──(merge)──► main
upstream/1.x  ──(rename.sh)──► upstream-renamed-1.x ──(merge)──► 1.x
```

| Branch | Content | Modified by |
|---|---|---|
| `main` | 2.x line (Jakarta Messaging, `jakarta.jms`) | us |
| `1.x` | 1.x line (JMS 2.0, `javax.jms`) | us |
| `upstream-renamed` | renamed upstream snapshots for `main` | `sync-upstream.sh` only |
| `upstream-renamed-1.x` | renamed upstream snapshots for `1.x` | `sync-upstream.sh` only |

Each commit on an `upstream-renamed*` branch is "an upstream tag or commit, passed through
`rename.sh`". It records the upstream commit in an `Upstream-Commit:` trailer.

### Development flow

* Develop on `main`, then cherry-pick to `1.x` if the change applies there:
  ```bash
  git switch 1.x
  git cherry-pick -x <commit>    # -x adds "(cherry picked from commit ...)" to the message
  ```
* Changes touching the JMS API may need adapting (`jakarta.jms` vs `javax.jms`).
* Upstream changes are never cherry-picked: each branch gets them from its own sync.
* `main` and `1.x` are never merged into each other.

## Why merging upstream works

If we merged `upstream/main` directly, git would see every file as renamed and conflict everywhere.
Instead, upstream is renamed first (on `upstream-renamed`), then merged.
Both sides of the merge use the new names, and the merge base is the previous sync commit,
so git only sees what upstream actually changed.
Conflicts only happen where we changed the same lines as upstream.

Lines that upstream changes at each release are normalized by `rename.sh`
(project versions become `${revision}`, the SCM tag becomes `HEAD`),
so upstream version bumps never conflict with our versions.

## The scripts

### `scripts/rename.sh`

Runs at the root of an unmodified upstream tree and transforms it in place:

1. Replaces names in file contents: packages (`org.apache.qpid.jms` → `com.rabbitmq.client.jms`,
   also in `/` form for `META-INF/services` paths), Maven coordinates, module names.
2. Replaces project versions with `${revision}` and the SCM tag with `HEAD` in POMs.
3. Moves module directories (`qpid-jms-client` → `jms-client`, ...).
4. Moves package directories (`org/apache/qpid/jms` → `com/rabbitmq/client/jms`).
5. Checks that no old name is left and that all POMs use `${revision}`, fails otherwise.

The output must stay deterministic: the same upstream tree always gives the same result.
The module list is hard-coded: if upstream adds, removes, or renames a module, the script
fails and must be updated.

### `scripts/sync-upstream.sh <upstream-ref> [<renamed-branch>]`

Adds a commit to `upstream-renamed` (default) or `upstream-renamed-1.x`:

1. Fetches `upstream` and `origin`, aligns the local renamed branch with `origin`.
2. Checks out the renamed branch in a temporary directory (`git worktree`),
   so your current checkout is not touched.
3. Replaces the content with the upstream ref (`git read-tree -u --reset <sha>`: sets the files
   and the index to the exact upstream tree, without moving the branch).
4. Runs `rename.sh` and commits.

It does nothing if the ref is already synced. It does not merge nor push.
It uses the `rename.sh` of your current checkout.

## Merging changes from upstream

### One-time setup (fresh clone)

```bash
git clone git@github.com:rabbitmq/rabbitmq-amqp-jms-client.git
cd rabbitmq-amqp-jms-client
git remote add upstream https://github.com/apache/qpid-jms.git
git switch -c 1.x --track origin/1.x   # explicit: "git switch 1.x" is ambiguous with upstream/1.x
git switch main
```

### `main`

```bash
git switch main
git pull
scripts/sync-upstream.sh 2.12.0                # upstream tag or commit, e.g. upstream/main
git log --oneline <previous-sha>..<new-sha>    # optional: review upstream changes (printed by the script)
git merge upstream-renamed
# resolve conflicts if any (see below)
./mvnw clean verify
git push origin main upstream-renamed
```

### `1.x`

Same steps, with the `1.x` branches:

```bash
git switch 1.x
git pull
scripts/sync-upstream.sh 1.18.0 upstream-renamed-1.x
git merge upstream-renamed-1.x
# resolve conflicts if any (see below)
./mvnw clean verify
git push origin 1.x upstream-renamed-1.x
```

### Conflicts

`git status` lists the conflicting files. For each one: edit the file, keep what makes sense
from both sides, then `git add <file>`. Finish with `git commit` (no message needed,
git proposes one). To start over: `git merge --abort`.

Useful commands:

* `git checkout --ours <file>`: keep our version of the whole file.
* `git checkout --theirs <file>`: keep upstream's version of the whole file.
* `git diff`: show the remaining conflicts.

Where to expect conflicts (files we changed):

| File | What to do |
|---|---|
| Root `pom.xml` | Keep our changes (no `org.apache:apache` parent, metadata, plugins, profiles) and take upstream's other changes (dependency versions, etc.). |
| Root `pom.xml`, `<parent>` block | Upstream bumped the `org.apache:apache` parent: keep ours (no parent). Optionally update our `*-plugin-version` properties to match the new parent. |
| Module `pom.xml` files | Keep our `<name>` and `Automatic-Module-Name`, take the rest. |
| `README.md`, `NOTICE`, `jms-client-dist/src/main/assembly/NOTICE` | Keep ours. For `NOTICE`, keep upstream's changes in the Apache part (e.g. copyright year). |
| `JmsConnection.java`, `MetaDataSupport.java` | Keep our product name, take upstream's other changes. |
| `.asf.yaml`, `appveyor.yml` (deleted in the fork) | "modify/delete" conflict: `git rm <file>`. |

### If `sync-upstream.sh` fails

`rename.sh` failed: nothing was committed. Usually upstream added, removed, or renamed
a module, or introduced a new form of the old names.
Update `rename.sh` on `main` (cherry-pick to `1.x` if needed), commit, and run the sync again.

## Rules

* Never merge `upstream/main` or `upstream/1.x` directly: always go through `sync-upstream.sh`.
* Never commit manually on `upstream-renamed` or `upstream-renamed-1.x`.
* Never push tags in bulk (`git push --tags`, `--follow-tags`): the local repository contains
  all upstream tags (`2.11.0`, `1.17.0`, ...), they do not belong here.
  Our release tags use a `v` prefix (`v2.12.0`) to avoid clashes with upstream tags.
