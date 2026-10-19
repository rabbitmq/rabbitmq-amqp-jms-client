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
pushes to `main`, or a manual run on `main`, build and publish to `gh-pages`.
The release workflow checks out the release tag before invoking the same script.
Publication and release workflows share a concurrency group to serialize updates.

The script preserves existing versions and writes:

- `<version>/htmlsingle/index.html` for the version in the checked-out POM
- `2.x/snapshot/htmlsingle/index.html` for snapshots
- `2.x/milestone/htmlsingle/index.html` for prereleases
- `2.x/stable/htmlsingle/index.html` for releases

The site root links to the available channels. The script creates `gh-pages` on
first publication and uses a temporary worktree, leaving the source checkout in
place. Publishing requires Git 2.42 or later and write access to `origin`.

Configure GitHub Pages to deploy from the root of the `gh-pages` branch. The
workflow needs permission to push that branch and request a Pages build. The
workflows explicitly request a build through the GitHub API because pushes using
`GITHUB_TOKEN` do not trigger Pages builds automatically. This setup currently publishes
only the 2.x line; the `1.x` branch can adopt it separately.

The JMS broker documentation link temporarily points to its internal source.
Replace it with the published, version-appropriate URL when available.
