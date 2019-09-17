# Comunica SPARQL AMF Init Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-init-sparql-amf.svg)](https://www.npmjs.com/package/@comunica/actor-init-sparql-amf)

A comunica SPARQL SmartKG Init Actor.

This module is part of the [Comunica framework](https://github.com/comunica/comunica).

## Install

```bash
$ yarn add @comunica/actor-init-sparql-smartkg
```

## Usage

Show 100 triples from http://fragments.dbpedia.org/2015-10/en:

```bash
$ comunica-sparql-smartkg http://fragments.dbpedia.org/2015-10/en "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 100"
```

Show the help with all options:

```bash
$ comunica-sparql-amf --help
```

Just like [Comunica SPARQL](https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql),
a [dynamic variant](https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql#usage-from-the-command-line) (`comunica-dynamic-sparql-smartkg`) also exists,
and [a script for starting a HTTP service](https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql#usage-from-http) (`comunica-sparql-smartkg-http`).
