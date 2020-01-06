# PaySim Demo for Neo4j

It's hard to research and study fraud, especially in the financial
services realm, without being a bank:

- Where do you get the data?
- What does fraud even look like?
- How do I test ideas about detection or prediction?

![A simplified view of the data mode](./simplified-data-model.png?raw=true)
> A simplified view of the data model

Sure, you could draft a resume and send it to one of the many banks,
but luckily for those of us that are either just curious or more in
the research/academic realm, this demo can help (to some degree).

## Background

### What's PaySim?

PaySim was originally developed to solve the problem of researchers
getting their hands on "realistic" transactional data to test their
ideas. In the words of the author, Edgar A. Lopez-Rojas:

> PaySim uses aggregated anonymized data from a real financial dataset
> to generate synthetic data that closely resembles the transactions
> dynamics, statistical properties and causal dynamics observed in the
> original dataset, while incorporating any malicious behaviour of
> interest. (https://link.springer.com/chapter/10.1007/978-3-030-22868-2_51)

Specifically, PaySim models a _mobile money_ network. This is similar
to, but fundamentally different than traditional credit card fraud
focused on heavily in the US, Canadian, and EU markets.

For more details, check out: https://en.wikipedia.org/wiki/Mobile_payment

### What's this Project?

I've since taken PaySim, made various incremental
[improvements](https://github.com/voutilad/paysim), and wrapped it up
in a self-contained driver that populates a target Neo4j
database. I've essentially done the heavy lifting for you to go from
zero to graph in minutes.

### Wait, what's Neo4j?

The world's most flexible, reliable, and developer friendly native
graph database trusted by the top financial service
institutions. (Learn more at https://neo4j.com)

> And full disclosure: my current employer ;-)

For an overview of an example Neo4j fraud detection solution stack,
check out this talk from Jennifer Reif:

  "Building a Full-Stack Fraud Detection Solution with Kafka, GraphQL,
  and Neo4j Graph Algorithms", uploaded Nov 18, 2019.
  https://www.youtube.com/watch?v=lDgt2sN3Kyo

## Installation

While the project depends on a few other[2] Java projects, it's
designed to be standalone in terms of installation and usage. You'll
need to build from source, however. (See the Licensing section for
details why.)

Make sure you get and install:

- A recent JDK 11 (I haven't tested 8) - https://adoptopenjdk.net
- Neo4j 3.5 - https://neo4j.com/download
- Leiningen - https://leiningen.org/

### Building from Source

1. You'll need a recent version of [Leiningen](https://leiningen.org/)
2. Either clone this project or grab a source release
3. Since the PaySim dependency isn't published in a public repo,
   install it locally by running:
   ```sh
   $ lein deploy local-paysim-jar org.paysim/paysim 2.0-voutilad-5 ./lib/paysim-2.0-voutilad-5.jar
   ```
4. Build the uberjar using:
   ```sh
   $ lein uberjar
   ```
5. You should now be able to run the simulation demo via:
   ```sh
   $ java jar ./target/uberjar/paysim-neo4j-0.1.0-standalone.jar
   ```

## Usage

If you're running a local Neo4j instance via something like Neo4j
Desktop, you will only need to provide the `password` for the `neo4j` user.

```
NAME:
 paysim-neo4j - Run a PaySim simulation and populate a target Neo4j database.

USAGE:
 paysim-neo4j [global-options] command [command options] [arguments...]

VERSION:
 0.1.0

COMMANDS:
   run                  Run the simulation and load the data.

GLOBAL OPTIONS:
       --uri S              bolt://localhost:7687  Bolt URI to a target Neo4j instance
   -u, --username S         neo4j                  Username for connecting to Neo4j
   -p, --password S         password               Password for Neo4j user
       --tls F              false                  Use TLS (encryption) for connecting to Neo4j?
       --trust-all-certs F  false                  Trust all certificates? (Warning: only use for local certs you trust.)
   -?, --help
```

## Caveats / Known Issues

- PaySim only simulates up to 30 days of transactions/behavior due to
  reliance on aggregate historical data to drive statistical modeling

- The transaction amounts are in an unspecified currency by design to
  (supposedly) help mask the original source of the real life
  aggregate data obtained by the original PaySim author(s).

- There are some known synchronization points currently, specifically
  for some logging output to stdout. I haven't profiled the code at
  all, so I know it could be faster.

## License

The original PaySim project is licensed under GPLv3 without a
classpath exception, so this work that distributes it in the provided
jar (in `./lib`) is currently considered a GPLv3 work.

This does pose some difficulties, namely incompatability with the
EPLv1 and EPLv2 licenses, of which Clojure and most of the Clojure
ecosystem use.

As such, this project exists in a horrible edge-case where neither you
nor I can redistribute a fully bundled work including the Clojure
runtime and PaySim. ¯\_(ツ)_/¯ (AFAIK only the code can be shared.)

An explanation: https://youtu.be/UqcONoahlmQ?t=9

## Footnotes

- [1] -- See the GSMA's mobile money metrics website:
  https://www.gsma.com/mobilemoneymetrics/#deployment-tracker

- [2] -- Specifically two forks:
  a) https://github.com/voutilad/paysim
  b) https://github.com/voutilad/mason
