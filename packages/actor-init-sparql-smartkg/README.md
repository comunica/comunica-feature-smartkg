# Comunica SPARQL AMF Init Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-init-sparql-smartkg.svg)](https://www.npmjs.com/package/@comunica/actor-init-sparql-smartkg)
[![Docker Pulls](https://img.shields.io/docker/pulls/comunica/actor-init-sparql-smartkg.svg)](https://hub.docker.com/r/comunica/actor-init-sparql-smartkg/)

Comunica SPARQL SmartKG is a SPARQL query engine for JavaScript that can query using the [Smart-KG approach](https://dl.acm.org/doi/abs/10.1145/3366423.3380177).

This module is part of the [Comunica framework](https://comunica.dev/).

**Warning: this is experimental software that may have breaking changes in the future**

**Limitation: only works in Node.js, not the browser environment**

## Install

```bash
$ yarn add @comunica/actor-init-sparql-smartkg
```

or

```bash
$ npm install -g @comunica/actor-init-sparql-smartkg
```

## Usage

Show 100 triples from http://fragments.dbpedia.org/2015-10/en:

```bash
$ comunica-sparql-smartkg http://fragments.dbpedia.org/2015-10/en "CONSTRUCT WHERE { ?s ?p ?o } LIMIT 100"
```

Show the help with all options:

```bash
$ comunica-sparql-smartkg --help
```

Just like [Comunica SPARQL](https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql),
a [dynamic variant](https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql#usage-from-the-command-line) (`comunica-dynamic-sparql-smartkg`) also exists.

_[**Read more** about querying from the command line](https://comunica.dev/docs/query/getting_started/query_cli/)._

### Usage within application

This engine can be used in JavaScript/TypeScript applications as follows:

```javascript
const newEngine = require('@comunica/actor-init-sparql-link-smartlg').newEngine;
const myEngine = newEngine();

const result = await myEngine.query(`
  SELECT DISTINCT * WHERE {
      ?s ?p ?o
  }`, {
  sources: ['http://fragments.dbpedia.org/2015-10/en'],
});

// Consume results as a stream (best performance)
result.bindingsStream.on('data', (binding) => {
    console.log(binding.get('?s').value);
    console.log(binding.get('?s').termType);

    console.log(binding.get('?p').value);

    console.log(binding.get('?o').value);
});

// Consume results as an array (easier)
const bindings = await result.bindings();
console.log(bindings[0].get('?s').value);
console.log(bindings[0].get('?s').termType);
```

_[**Read more** about querying an application](https://comunica.dev/docs/query/getting_started/query_app/)._

### Usage as a SPARQL endpoint

Start a webservice exposing http://fragments.dbpedia.org/2015-10/en via the SPARQL protocol, i.e., a _SPARQL endpoint_.

```bash
$ comunica-sparql-smartkg-http http://fragments.dbpedia.org/2015-10/en
```

Show the help with all options:

```bash
$ comunica-sparql-smartkg-http --help
```

The SPARQL endpoint can only be started dynamically.
An alternative config file can be passed via the `COMUNICA_CONFIG` environment variable.

Use `bin/http.js` when running in the Comunica monorepo development environment.

_[**Read more** about setting up a SPARQL endpoint](https://comunica.dev/docs/query/getting_started/setup_endpoint/)._

