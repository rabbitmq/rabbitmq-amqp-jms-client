# Building and publishing the guide

The RabbitMQ guide is written in AsciiDoc. Its Maven build is independent of the
library reactor to keep upstream merges small. The upstream Markdown reference
remains in `jms-client-docs`.

## Local build

With Java 11 or later and Python 3 installed, run from the repository root:

```bash
ci/build-documentation.sh
```

Open `docs/target/htmlsingle/index.html` in a browser. The HTML embeds its stylesheet
and does not require online fonts or scripts.

The build reads the library version, Maven coordinates, and Java baseline from the
root and client POMs, and writes generated AsciiDoc attributes under `docs/target`.
Use `{project-version}`, `{project-group-id}`, `{project-artifact-id}`, and
`{java-version}` in the guide. `{source-revision}` identifies the source commit.
The standalone documentation POM's version describes the build project, not the
library; do not update it when releasing the library.

Additional sections can be included from `asciidoc/index.adoc`. Java snippets can
also be included from files in the same checkout. Missing includes and attributes
fail the documentation build.

## Publishing

The documentation workflow builds pull requests without publishing. Relevant
pushes or manual runs on `main` and `1.x` build and publish to the same `gh-pages`
branch once this setup is present on each source branch.
The release workflow checks out the release tag before invoking the same script.
Publication and release workflows share a concurrency group across both branches
to serialize updates to the site.

The script preserves existing versions and writes:

- `<version>/htmlsingle/index.html` for the version in the checked-out POM
- `<major>.x/snapshot/htmlsingle/index.html` for snapshots
- `<major>.x/milestone/htmlsingle/index.html` for prereleases
- `<major>.x/stable/htmlsingle/index.html` for releases

The version line comes from the checked-out POM: `2.12.0-SNAPSHOT` publishes to
`2.x/snapshot`, while `1.18.0-SNAPSHOT` publishes to `1.x/snapshot`. Publishing one
line preserves the other line's pages, and the site root links to the available
channels of every published line.

The script creates `gh-pages` on first publication and uses a temporary worktree,
leaving the source checkout in place. Publishing requires Git 2.42 or later and
write access to `origin`.

Configure GitHub Pages to deploy from the root of the `gh-pages` branch. The
workflow needs permission to push that branch and request a Pages build. The
workflows explicitly request a build through the GitHub API because pushes using
`GITHUB_TOKEN` do not trigger Pages builds automatically. Both client lines use
the same Pages configuration.

To adopt this setup on `1.x`, cherry-pick the documentation changes and adapt the
guide's API wording to JMS 2.0 and `javax.jms`. The scripts and workflows can be
identical on both branches; the guide content belongs to its source branch.

The JMS broker documentation link temporarily points to its internal source.
Replace it with the published, version-appropriate URL when available.
