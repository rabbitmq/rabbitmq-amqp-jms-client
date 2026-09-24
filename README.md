# RabbitMQ AMQP JMS Client

RabbitMQ AMQP JMS Client is a JMS client that uses the AMQP 1.0 protocol, enabling it
to interact with RabbitMQ and other AMQP 1.0 servers.

It is a fork of [Apache Qpid JMS](https://github.com/apache/qpid-jms), released under
the same license ([Apache License 2.0](LICENSE)).
It is not affiliated with or endorsed by the Apache Software Foundation.

Not to be confused with the [RabbitMQ JMS Client](https://github.com/rabbitmq/rabbitmq-jms-client),
which uses AMQP 0-9-1.

## Maven coordinates

```xml
<dependency>
  <groupId>com.rabbitmq.client</groupId>
  <artifactId>jms-client</artifactId>
  <version>${jms-client.version}</version>
</dependency>
```

The root package is `com.rabbitmq.client.jms`. Classes keep their upstream names,
e.g. the JNDI initial context factory is `com.rabbitmq.client.jms.jndi.JmsInitialContextFactory`.

## Differences with Apache Qpid JMS

| | Apache Qpid JMS | RabbitMQ AMQP JMS Client |
|---|---|---|
| Maven coordinates | `org.apache.qpid:qpid-jms-client` | `com.rabbitmq.client:jms-client` |
| Root package | `org.apache.qpid.jms` | `com.rabbitmq.client.jms` |
| Module directories | `qpid-jms-*` | `jms-client-*` |

## Building the code

The project requires Maven 3.9 or more, the Maven wrapper (`./mvnw`) can be used instead
of a local installation. Some example commands follow.

Clean previous builds output and install all modules to local repository without
running the tests:

    ./mvnw clean install -DskipTests

Install all modules to the local repository after running all the tests:

    ./mvnw clean install

Perform a subset tests on the packaged release artifacts without
installing:

    ./mvnw clean verify -Dtest=TestNamePattern*

Execute the tests and produce code coverage report:

    ./mvnw clean test jacoco:report

The project version is defined by the `revision` property in the root POM and can be
overridden on the command line, e.g. `./mvnw install -Drevision=2.12.0`.

## Examples

First build and install all the modules as detailed above (if running against
a source checkout/release, rather than against released binaries) and then
consult the README in the jms-client-examples module itself.

## Documentation

Documentation source can be found in the jms-client-docs module.

## Distribution assemblies

After building the modules, src and binary distribution assemblies can be found at:

    jms-client-dist/target

## Publishing

The parent POM, `jms-client`, and `jms-client-discovery` are published to Maven Central
(Central Portal, deployments must be published manually from the portal).
The other modules are not published.

* Snapshots: run the "Publish snapshot" GitHub Actions workflow.
* Releases: update [`release-versions.txt`](release-versions.txt), then run the
  "Release RabbitMQ AMQP JMS Client" workflow. [`ci/release.sh`](ci/release.sh)
  sets the `revision` property to the release version, tags (`v<version>`), deploys,
  and sets the next development version.

Release tags use a `v` prefix, upstream Apache Qpid JMS tags do not.
Do not push upstream tags to this repository (e.g. with `git push --tags`).

## Synchronization with upstream

Changes from Apache Qpid JMS are brought in with a dedicated branch:

* `upstream-renamed` contains upstream snapshots transformed by
  [`scripts/rename.sh`](scripts/rename.sh) (packages, Maven coordinates, module directories).
  It is never modified manually.
* [`scripts/sync-upstream.sh <upstream-ref>`](scripts/sync-upstream.sh) adds a commit to
  `upstream-renamed` for a given upstream tag or commit, e.g. `2.12.0`.
* `upstream-renamed` is then merged into `main`.

The `1.x` branch follows the same process with the `upstream-renamed-1.x` branch.
Never merge upstream branches directly.

See [FORK.md](FORK.md) for details.
