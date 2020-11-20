<p align="center">
  <a href="https://comunica.dev/">
    <img alt="Comunica" src="https://comunica.dev/img/comunica_red.svg" width="200">
  </a>
</p>

<p align="center">
  <strong>Smart-KG in Comunica</strong>
</p>

<p align="center">
<a href="https://travis-ci.com/comunica/comunica-feature-smartkg"><img src="https://travis-ci.com/comunica/comunica-feature-smartkg.svg?branch=master" alt="Build Status"></a>
<a href="https://coveralls.io/github/comunica/comunica-feature-smartkg?branch=master"><img src="https://coveralls.io/repos/github/comunica/comunica-feature-smartkg/badge.svg?branch=master" alt="Coverage Status"></a>
<a href="https://gitter.im/comunica/Lobby"><img src="https://badges.gitter.im/comunica.png" alt="Gitter chat"></a>
</p>

**[Learn more about Comunica on our website](https://comunica.dev/).**

This is a monorepo that contains packages for allowing [Comunica](https://github.com/comunica/comunica) to handle [Smart-KG](https://dl.acm.org/doi/abs/10.1145/3366423.3380177) sources.
If you want to _use_ an Smart-KG-enabled Comunica engine, have a look at [Comunica SPARQL Smart-KG](https://github.com/comunica/comunica-feature-smartkg/tree/master/packages/actor-init-sparql-smartkg).

Concretely, this monorepo adds Smart-KG support to Comunica using the following packages:
* [Comunica SPARQL AMF Init Actor](https://github.com/comunica/comunica-feature-smartkg/tree/master/packages/actor-init-sparql-smartkg): Init actor for query against Smart-KG servers.
* [Comunica BGP SmartKG Query Operation Actor](https://github.com/comunica/comunica-feature-smartkg/tree/master/packages/actor-query-operation-bgp-smartkg): A BGP Smart-KG query operation actor.

## Development Setup

_(JSDoc: https://comunica.github.io/comunica-feature-smartkg/)_

This repository should be used by Comunica module **developers** as it contains multiple Comunica modules that can be composed.
This repository is managed as a [monorepo](https://github.com/babel/babel/blob/master/doc/design/monorepo.md)
using [Lerna](https://lernajs.io/).

If you want to develop new features
or use the (potentially unstable) in-development version,
you can set up a development environment for Comunica.

Comunica requires [Node.JS](http://nodejs.org/) 8.0 or higher and the [Yarn](https://yarnpkg.com/en/) package manager.
Comunica is tested on OSX, Linux and Windows.

This project can be setup by cloning and installing it as follows:

```bash
$ git clone https://github.com/comunica/comunica.git
$ cd comunica
$ yarn install
```

**Note: `npm install` is not supported at the moment, as this project makes use of Yarn's [workspaces](https://yarnpkg.com/lang/en/docs/workspaces/) functionality**

This will install the dependencies of all modules, and bootstrap the Lerna monorepo.
After that, all [Comunica packages](https://github.com/comunica/comunica-feature-smartkg/tree/master/packages) are available in the `packages/` folder
and can be used in a development environment, such as querying with [Comunica SPARQL Smart-KG (`packages/actor-init-sparql-smartkg`)](https://github.com/comunica/comunica-feature-link-traversal/tree/master/packages/actor-init-sparql-smartkg).

Furthermore, this will add [pre-commit hooks](https://www.npmjs.com/package/pre-commit)
to build, lint and test.
These hooks can temporarily be disabled at your own risk by adding the `-n` flag to the commit command.

## License
This code is copyrighted by [WU Vienna University of Economics and Business — Institute for Information Business](https://www.wu.ac.at/en/infobiz/) and [Ghent University – imec](http://idlab.ugent.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
