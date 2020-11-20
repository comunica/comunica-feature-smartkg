# Comunica BGP SmartKG Query Operation Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-query-operation-bgp-smartkg.svg)](https://www.npmjs.com/package/@comunica/actor-query-operation-bgp-smartkg)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor
that handles [SPARQL Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
using the [Smart-KG client-side algorithm](https://dl.acm.org/doi/abs/10.1145/3366423.3380177).

Since this actor uses the filesystem for caching and referencing HDT files, it is only usable in Node.JS,
not in browser environments.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-query-operation-bgp-smartkg
```

## Configure

This package exposes two variants of the Smart-KG algorithm:
1. `ActorQueryOperationBgpSmartkgDeepJoin`: Handles joins between quad patterns in a left-deep manner. (default Smart-KG algorithm)
2. `ActorQueryOperationBgpSmartkgStarJoin`: Resolves all star patterns seperately, and joins them aftwards. (This actor should only be used if the number of intermediary results for all star patterns is low to avoid expensive joins)

After installing, this package can be added to your engine's configuration as follows:

### `ActorQueryOperationBgpSmartkgDeepJoin`

```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-query-operation-bgp-smartkg/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "config-caisskg:sparql-queryoperators.json#mySmartkgBgpQueryOperator",
      "@type": "ActorQueryOperationBgpSmartkgDeepJoin",
      "cbqo:mediatorQueryOperation": { "@id": "config-sets:sparql-queryoperators.json#mediatorQueryOperation" },
      "caqobs:mediatorHttp": { "@id": "config-sets:http.json#mediatorHttp" },
      "caqobs:mediatorJoin": { "@id": "config-sets:sparql-queryoperators.json#mediatorRdfJoin" },
      "beforeActor": "config-sets:sparql-queryoperators.json#myLeftDeepSmallestBgpQueryOperator"
    }
  ]
}
```

### `ActorQueryOperationBgpSmartkgStarJoin`

```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-query-operation-bgp-smartkg/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "config-caisskg:sparql-queryoperators.json#mySmartkgBgpQueryOperator",
      "@type": "ActorQueryOperationBgpSmartkgStarJoin",
      "cbqo:mediatorQueryOperation": { "@id": "config-sets:sparql-queryoperators.json#mediatorQueryOperation" },
      "caqobs:mediatorHttp": { "@id": "config-sets:http.json#mediatorHttp" },
      "caqobs:mediatorJoin": { "@id": "config-sets:sparql-queryoperators.json#mediatorRdfJoin" },
      "beforeActor": "config-sets:sparql-queryoperators.json#myLeftDeepSmallestBgpQueryOperator"
    }
  ]
}
```
