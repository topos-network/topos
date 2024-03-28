<div id="top"></div>
<!-- PROJECT LOGO -->
<br />
<div align="center">

  <img src="../.github/assets/topos_logo.png#gh-light-mode-only" alt="Logo" width="200">
  <img src="../.github/assets/topos_logo_dark.png#gh-dark-mode-only" alt="Logo" width="200">

  <h3 align="center">Topos Smart Contracts</h3>

  <p align="center">
    Solidity smart contracts for Topos Protocol and Topos Messaging Protocol
  </p>
</div>

<div align="center">

![analyze](https://github.com/topos-protocol/topos/actions/workflows/contract-analyze.yml/badge.svg)
![build](https://github.com/topos-protocol/topos/actions/workflows/contract-build.yml/badge.svg)
![docker-build](https://github.com/topos-protocol/topos/actions/workflows/contract-docker-build-push.yml/badge.svg)
![format](https://github.com/topos-protocol/topos/actions/workflows/contract-format.yml/badge.svg)
![lint](https://github.com/topos-protocol/topos/actions/workflows/contract-lint.yml/badge.svg)
![test](https://github.com/topos-protocol/topos/actions/workflows/contract-test.yml/badge.svg)
![npm](https://img.shields.io/npm/v/@topos-protocol/topos.svg) <!-- TODO: update -->
![release](https://img.shields.io/github/v/release/topos-protocol/topos) <!-- TODO: update -->
[![](https://dcbadge.vercel.app/api/server/7HZ8F8ykBT?style=flat)](https://discord.gg/7HZ8F8ykBT)

</div>

## Description

This repository contains Solidity smart contracts to be used with the Topos Protocol in the Topos ecosystem. The contract compilation/deployment and testing methods are taken care by the **Hardhat** development framework.

## Installation

To install the project along with all the dependencies run:

```
$ npm install
```

## Dependencies

This project contains some smart contracts which inherit from [OpenZeppelin contracts](https://github.com/OpenZeppelin/openzeppelin-contracts). This should be installed automatically.

## Build

To build:

```
$ npm run build
```

## Tests

To run the tests:

```
$ npm run test
```

## Coverage

To see the test coverage:

```
npm run coverage
```

## Linting

For formatting this project uses `prettier` with the `prettier-plugin-solidity` plugin. For general style guide and security checks this project uses `Solhint`.

To run linter:

```
$ npm run lint
```

To fix the format:

```
$ npm run lint:fix
```

## Deployment

### Deployment of a single contract with CREATE2

A NodeJS script is made available to deploy contracts with `CREATE2`, i.e., with constant addresses. The script is to be used with a deployed instance of `ConstAddressDeployer`. See an example below:

```
$ npm run deploy http://myChainRPCEndpoint myCompiledContract.json MySecretSalt ACustomGasLimit|null MyConstructorArg AnotherConstructorArg

E.g.
$ npm run deploy http://127.0.0.1:8545 artifacts/contracts/topos-core/ToposCore.sol/ToposCore.json $TOPOS_CORE_SALT 2000000 0xF121424e3F7d73fCD79DcBCA67E8F10BeBE67b00 0x3100000000000000000000000000000000000000000000000000000000000000
```

### Deployment of the full Topos Messaging Protocol

To deploy the full Topos Messaging Protocol, another `deploy:topos-msg-protocol` npm script is available. This scripts deploys the following contracts:

- `TokenDeployer` with constant address
- `ToposCore` with constant address
- `ToposCoreProxy` with constant address
- `ToposMessaging` with constant address

```
$ npm run deploy:topos-msg-protocol http://myChainRPCEndpoint pathToSequencerPrivateKey

E.g.
$ npm run deploy:topos-msg-protocol http://127.0.0.1:8545 /data/node-1/consensus/validator.key
```

This script requires a few environment variables to be set:

- TOKEN_DEPLOYER_SALT: salt for the `TokenDeployer` contract
- TOPOS_CORE_SALT: salt for the `ToposCore` contract
- TOPOS_CORE_PROXY_SALT: salt for the `ToposCoreProxy` contract
- TOPOS_MESSAGING_SALT: salt for the `ToposMessaging` contract
- PRIVATE_KEY: the private key of the account to be used to deploy contracts

### Deployment of the contracts on dynamic addresses

To deploy the full Topos Messaging Protocol on dynamic contract addresses (could be any Ethereum compatible network, not just Polygon Edge network with predeployed const address deployer contract), `deploy:topos-msg-protocol-dynamic` npm script is available. This script is intended for usage primarily during development. It deploys the following contracts:

- `TokenDeployer`
- `ToposCore`
- `ToposCoreProxy`
- `ToposMessaging`

```
$ npm run deploy:topos-msg-protocol-dynamic http://myChainRPCEndpoint sequencerPrivateKey

E.g.
$ npm run deploy:topos-msg-protocol-dynamic http://127.0.0.1:8545 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

This script requires a few environment variables to be set:

- PRIVATE_KEY: the private key of the account to be used to deploy contracts

## Docker

Some of the above commands can be run in docker.

```
$ docker build . --t target [build|test|lint]
```

## License

This project is released under the terms of the MIT license.
