![topos](./.github/assets/topos_logo_dark.png)
## [0.1.0](https://github.com/topos-protocol/topos/compare/v0.0.11..0.1.0) - 2024-03-25

### ⛰️  Features

- Update smart-contracts to 3.4.0 stable - ([a714493](https://github.com/topos-protocol/topos/commit/a714493dd5aaf235d99f4b903f49a355e0c38d14))
- Add p2p layer health check ([#464](https://github.com/topos-protocol/topos/issues/464)) - ([d2ec941](https://github.com/topos-protocol/topos/commit/d2ec941ec24d3cbc27c11d4fcf6b0495911f85e2))
- Terminate stream if client is dropping the connection ([#463](https://github.com/topos-protocol/topos/issues/463)) - ([2c73f0b](https://github.com/topos-protocol/topos/commit/2c73f0bae1dc25aad504d45dcc789360cce7dbaa))
- Introduce topos-node crate ([#459](https://github.com/topos-protocol/topos/issues/459)) - ([d8db631](https://github.com/topos-protocol/topos/commit/d8db631d970d6b855e5a47f0c561abe8bab9832d))
- Add proper error handling to setup command ([#452](https://github.com/topos-protocol/topos/issues/452)) - ([3335846](https://github.com/topos-protocol/topos/commit/3335846327c9c0f32e85694861f30520a9f2a6c5))
- Remove dht publication ([#449](https://github.com/topos-protocol/topos/issues/449)) - ([7030341](https://github.com/topos-protocol/topos/commit/70303412f139c7fe5ca0d4775f367d27543ac791))
- Add benchmark dns option to spam subcommand ([#448](https://github.com/topos-protocol/topos/issues/448)) - ([90405f3](https://github.com/topos-protocol/topos/commit/90405f3f4bd468c33685158ddddb793b109e3f22))
- Move telemetry-otlp setup into telemetry crate ([#446](https://github.com/topos-protocol/topos/issues/446)) - ([8a15fc4](https://github.com/topos-protocol/topos/commit/8a15fc4c0aa07ba71ca8376f21056949d71e92c5))

### 🐛 Bug Fixes

- *(config)* Fix the parse of edge_path ENV var ([#482](https://github.com/topos-protocol/topos/issues/482)) - ([b2a1af0](https://github.com/topos-protocol/topos/commit/b2a1af06dfa6987261a08ccbc8f05ed1bdc0d0b8))
- *(p2p)* Accept listener connection during bootstrap ([#484](https://github.com/topos-protocol/topos/issues/484)) - ([b8cd730](https://github.com/topos-protocol/topos/commit/b8cd730c2e2a6d2799a5c741b026cf03d9eadd33))
- *(p2p)* Rework ticks of bootstrap query interval ([#483](https://github.com/topos-protocol/topos/issues/483)) - ([5b6ddb8](https://github.com/topos-protocol/topos/commit/5b6ddb80ded50525a27617ca5f7c911525752619))
- Bump smart contract version ([#478](https://github.com/topos-protocol/topos/issues/478)) - ([642203c](https://github.com/topos-protocol/topos/commit/642203c962ede91d821af2b54e5f7bc0d845d407))
- Concurrency insert between pending and delivered ([#467](https://github.com/topos-protocol/topos/issues/467)) - ([bd5e3f5](https://github.com/topos-protocol/topos/commit/bd5e3f52ba00bafa25e0f3ce8b42b326e4fb5ef0))
- Block handling during certificate generation ([#471](https://github.com/topos-protocol/topos/issues/471)) - ([a5299c8](https://github.com/topos-protocol/topos/commit/a5299c80068d6612d1aba162556f9ccd3dd3d0a8))
- Update mio ([#473](https://github.com/topos-protocol/topos/issues/473)) - ([8291740](https://github.com/topos-protocol/topos/commit/82917405e7bf102c06194caadc973f57ac735649))
- Revert update smart contract event ([#470](https://github.com/topos-protocol/topos/issues/470)) - ([c41a51a](https://github.com/topos-protocol/topos/commit/c41a51a2ff86198f44dd6b24ff534507a17cf519))
- Update smart contract event ([#462](https://github.com/topos-protocol/topos/issues/462)) - ([f995859](https://github.com/topos-protocol/topos/commit/f9958599a1da31d7c92a8e8a0e925e79ce6140cb))
- Remove duplicated certificate push on gossipsub ([#458](https://github.com/topos-protocol/topos/issues/458)) - ([b0e88dc](https://github.com/topos-protocol/topos/commit/b0e88dce2b7ea060ce34377b40a10e54edd16e02))
- Add next_pending_certificate on end task ([#455](https://github.com/topos-protocol/topos/issues/455)) - ([2aaa500](https://github.com/topos-protocol/topos/commit/2aaa50071ca14415bb0284930c404659b6c463d8))

### 🚜 Refactor

- Improve delivery timing ([#466](https://github.com/topos-protocol/topos/issues/466)) - ([96e862f](https://github.com/topos-protocol/topos/commit/96e862f5b886a38a5c67590d1e152ab9894d6f15))
- Store instantiation ([#461](https://github.com/topos-protocol/topos/issues/461)) - ([213b8d4](https://github.com/topos-protocol/topos/commit/213b8d482cf6e08ec0f1cae0e9dfd981b156a98d))
- Update error management and config/process ([#460](https://github.com/topos-protocol/topos/issues/460)) - ([cc0c7b5](https://github.com/topos-protocol/topos/commit/cc0c7b538d9f6b91c184db10eedd9d94c4f368fb))
- Move edge config to config crate ([#445](https://github.com/topos-protocol/topos/issues/445)) - ([23cc558](https://github.com/topos-protocol/topos/commit/23cc55887703bac01b7ec26486f47b03316046c1))
- Tce-broadcast config ([#444](https://github.com/topos-protocol/topos/issues/444)) - ([10c3879](https://github.com/topos-protocol/topos/commit/10c3879cd30bf0172996cfbf48ab5c991e767eaf))

### ⚙️ Miscellaneous Tasks

- Update changelog for 0.1.0 - ([65fc8cd](https://github.com/topos-protocol/topos/commit/65fc8cd05d1fdaecd809e92a0643dc02557ad460))
- Update changelog for 0.1.0 - ([a82617a](https://github.com/topos-protocol/topos/commit/a82617a6c653f02a00fc9565f2c5abb42c9b6c26))
- Disable coverage report on release branch (push) - ([09f3663](https://github.com/topos-protocol/topos/commit/09f36639ef62a02a2a84bde8f36a98ce6274ea6f))
- Disable coverage report on release branch (push) - ([e909e22](https://github.com/topos-protocol/topos/commit/e909e22d6dac251e4026816cd8dd5c84851e9db5))
- Disable coverage report on release branch ([#481](https://github.com/topos-protocol/topos/issues/481)) - ([8f10090](https://github.com/topos-protocol/topos/commit/8f10090094bf110670137f73a115bda54f64aba5))
- Update changelog for 0.1.0 - ([c68798e](https://github.com/topos-protocol/topos/commit/c68798eeed366a421a076cc1908aaca8013d80cf))
- Creating CHANGELOG.md for 0.0.11 - ([463f52f](https://github.com/topos-protocol/topos/commit/463f52feb73f10d2a194cf44863842a9f0cf13a0))
- Bumping version 0.1.0 - ([16de6a6](https://github.com/topos-protocol/topos/commit/16de6a675b0fe44afd20526202a2e5178b40994d))
- Update deps ([#474](https://github.com/topos-protocol/topos/issues/474)) - ([264c569](https://github.com/topos-protocol/topos/commit/264c5694980fded79ea0749d03f54a345d90c741))
- Refactor logs and fix typo ([#465](https://github.com/topos-protocol/topos/issues/465)) - ([8044310](https://github.com/topos-protocol/topos/commit/8044310b8ee330d5a14d509137dc4243cb2c2372))
- Removing cache_size ([#472](https://github.com/topos-protocol/topos/issues/472)) - ([b2e4cf8](https://github.com/topos-protocol/topos/commit/b2e4cf88ac0c0b2ee92b7ef120a4c4e97493150c))
- Backport fix of 0.0.11 ([#453](https://github.com/topos-protocol/topos/issues/453)) - ([53328ac](https://github.com/topos-protocol/topos/commit/53328acc813816757c57f3279cbd5f2aa738d2f0))

### Build

- Ignore pr checking name for release ([#480](https://github.com/topos-protocol/topos/issues/480)) - ([cfd8890](https://github.com/topos-protocol/topos/commit/cfd8890a0cb03f25fdaae8b181ab9c33f785e34e))

## [0.0.11](https://github.com/topos-protocol/topos/compare/v0.0.10..v0.0.11) - 2024-02-08

### ⛰️  Features

- Introduce topos-config crate ([#443](https://github.com/topos-protocol/topos/issues/443)) - ([4ff2a23](https://github.com/topos-protocol/topos/commit/4ff2a23e3a05ea3e950763bd4bde3d3ef6ef891b))
- Adding positions to certificate ([#440](https://github.com/topos-protocol/topos/issues/440)) - ([5315710](https://github.com/topos-protocol/topos/commit/531571025a4d81f9d9aa713ca12594756ca56a7e))
- Improve sequencer error handling and shutdown/restart sequence ([#428](https://github.com/topos-protocol/topos/issues/428)) - ([ab8bb9e](https://github.com/topos-protocol/topos/commit/ab8bb9e83afee545c3730f974ae8591c7fc70f3d))
- Update double echo to use pending CF ([#418](https://github.com/topos-protocol/topos/issues/418)) - ([8fb4003](https://github.com/topos-protocol/topos/commit/8fb4003d5579a8fee6d81c463707131959f076c3))
- Use anvil for sequencer tests ([#427](https://github.com/topos-protocol/topos/issues/427)) - ([5b0257b](https://github.com/topos-protocol/topos/commit/5b0257bed685c064c3eafbea2b5c77125e6c9041))
- Update tce config addresses ([#415](https://github.com/topos-protocol/topos/issues/415)) - ([476948f](https://github.com/topos-protocol/topos/commit/476948fa671b431bfa797aabf6b96949ac734db6))
- Remove the register commands macro ([#426](https://github.com/topos-protocol/topos/issues/426)) - ([985d0be](https://github.com/topos-protocol/topos/commit/985d0be0c75ddd1d41e94a172824b706ca0f9c5f))
- Run e2e topos integration workflow ([#408](https://github.com/topos-protocol/topos/issues/408)) - ([f0b7637](https://github.com/topos-protocol/topos/commit/f0b763786aa869454c9e30076ed08d0a456ed319))
- Adding filter on message when non validator ([#405](https://github.com/topos-protocol/topos/issues/405)) - ([b096482](https://github.com/topos-protocol/topos/commit/b0964825a5f386d75507482ee9068e26c9d74fe0))
- Add no-edge-process flag to node init ([#401](https://github.com/topos-protocol/topos/issues/401)) - ([28a553b](https://github.com/topos-protocol/topos/commit/28a553b6d17933bfbcca835640cc0c165bcd0124))
- Refactor peer selection for synchronization ([#382](https://github.com/topos-protocol/topos/issues/382)) - ([6982d33](https://github.com/topos-protocol/topos/commit/6982d336296a9b9ec5eacb025d938b6cb47b6e0a))
- Remove task manager channels ([#391](https://github.com/topos-protocol/topos/issues/391)) - ([f5fa427](https://github.com/topos-protocol/topos/commit/f5fa4276d8a524fd04bbf2a0d1d036d7f5af34bb))
- Add batch message and update double echo ([#383](https://github.com/topos-protocol/topos/issues/383)) - ([f0bc90c](https://github.com/topos-protocol/topos/commit/f0bc90c7480a84c0c12016e748f2a002477f4417))

### 🐛 Bug Fixes

- Fixing wrong use of IntCounterVec ([#442](https://github.com/topos-protocol/topos/issues/442)) - ([fe062a5](https://github.com/topos-protocol/topos/commit/fe062a5bdfa9ade2b94de88cbd6b3946b72a94c3))
- Clippy unused ([#434](https://github.com/topos-protocol/topos/issues/434)) - ([4aa6a9e](https://github.com/topos-protocol/topos/commit/4aa6a9e4723b8aaa2c2da0fdf32d8c8c926f7764))
- Remove an unused channel that was locking the broadcast ([#433](https://github.com/topos-protocol/topos/issues/433)) - ([43c6fe5](https://github.com/topos-protocol/topos/commit/43c6fe5caffd35103b20ec11d7ae2f35b1af98b9))
- RUSTSEC-2024-0003 ([#431](https://github.com/topos-protocol/topos/issues/431)) - ([cad4d76](https://github.com/topos-protocol/topos/commit/cad4d76ccf88be3f1399d371cf9dbdd7211343bc))
- RUSTSEC-2023-0078 ([#429](https://github.com/topos-protocol/topos/issues/429)) - ([35f8930](https://github.com/topos-protocol/topos/commit/35f8930056f641b33c56b4799b8c0cbe6f0a5eda))
- Fixing release notification ([#423](https://github.com/topos-protocol/topos/issues/423)) - ([7503fa7](https://github.com/topos-protocol/topos/commit/7503fa7f2385294f2a12a86eaa521af61fb9bc95))
- Move test abi generation to separate module ([#424](https://github.com/topos-protocol/topos/issues/424)) - ([d4ff358](https://github.com/topos-protocol/topos/commit/d4ff3581d43eabb14c3d977f34b69aee396e98d4))
- Return error from subprocesses ([#422](https://github.com/topos-protocol/topos/issues/422)) - ([53b3229](https://github.com/topos-protocol/topos/commit/53b3229b7e26b7bb9b3a8d97725e7cc86174df9b))

### 🚜 Refactor

- Graphql types to differentiate inputs ([#435](https://github.com/topos-protocol/topos/issues/435)) - ([4b0ec9b](https://github.com/topos-protocol/topos/commit/4b0ec9b2b3b6ab075d1b9bfde54dc8bb179bbde8))

### ⚙️ Miscellaneous Tasks

- Debug 0.0.11 synchronization ([#447](https://github.com/topos-protocol/topos/issues/447)) - ([edf86ee](https://github.com/topos-protocol/topos/commit/edf86ee32f8b34c5b11eecd5b0ed6fe5bdd0191a))
- Update dependencies ([#450](https://github.com/topos-protocol/topos/issues/450)) - ([62126e0](https://github.com/topos-protocol/topos/commit/62126e0417d8d4225eb8bb6eebb7fc0a0f526cc6))
- Remove mention of topos-api from coverage YAML ([#438](https://github.com/topos-protocol/topos/issues/438)) - ([6c7e342](https://github.com/topos-protocol/topos/commit/6c7e342715d86bcbd75e1bcd4054eb4cbeddf5cf))
- Bump version for topos to 0.0.11 ([#439](https://github.com/topos-protocol/topos/issues/439)) - ([917eaf9](https://github.com/topos-protocol/topos/commit/917eaf993336780a373dbcdfea4dd0d8b3e81f50))
- Refactor struct in topos-core ([#437](https://github.com/topos-protocol/topos/issues/437)) - ([acedac7](https://github.com/topos-protocol/topos/commit/acedac7e09094364b5406ee72f9a65241784c478))
- Update crates structure for api/uci and core ([#436](https://github.com/topos-protocol/topos/issues/436)) - ([355b08a](https://github.com/topos-protocol/topos/commit/355b08acf91052564dae65872904950d20b72ebd))
- Fix logs for pending ([#432](https://github.com/topos-protocol/topos/issues/432)) - ([342b2c7](https://github.com/topos-protocol/topos/commit/342b2c71ef0621a93b5f4460abd313f1c8b4c62b))
- Updating double echo for devnet ([#416](https://github.com/topos-protocol/topos/issues/416)) - ([1e91086](https://github.com/topos-protocol/topos/commit/1e91086a68ec01d304c5d8867e8fbcd671798599))
- Fixing clippy warning for 1.75.0 ([#425](https://github.com/topos-protocol/topos/issues/425)) - ([22a3745](https://github.com/topos-protocol/topos/commit/22a374506e087ab9cba68f9e0ed00682df6be6df))
- Refactor push certificate tests and cleanup regtest ([#399](https://github.com/topos-protocol/topos/issues/399)) - ([c60170d](https://github.com/topos-protocol/topos/commit/c60170dc19a9add84648afaf7962252ec77c160f))
- Update signal handle by tce ([#417](https://github.com/topos-protocol/topos/issues/417)) - ([beca28b](https://github.com/topos-protocol/topos/commit/beca28ba224b4a217a147f88142692acd1613a32))
- Cleanup topos tools ([#397](https://github.com/topos-protocol/topos/issues/397)) - ([0820306](https://github.com/topos-protocol/topos/commit/08203062a1cada7470d8b8207539d7a660ece466))
- Adding context on connection to self ([#413](https://github.com/topos-protocol/topos/issues/413)) - ([6e72999](https://github.com/topos-protocol/topos/commit/6e729992202ed4c56a1564cc39755ff1e0766ff8))
- Adding context on connection to self ([#411](https://github.com/topos-protocol/topos/issues/411)) - ([3a799ac](https://github.com/topos-protocol/topos/commit/3a799ac41542bd216a0512d9cf3a634a6ed2af07))
- Adding context to p2p msg received ([#410](https://github.com/topos-protocol/topos/issues/410)) - ([e1b2ccf](https://github.com/topos-protocol/topos/commit/e1b2ccf99a114a60e04c8ed187a8c9bd2cf066e4))
- Adding context to certificate broadcast ([#409](https://github.com/topos-protocol/topos/issues/409)) - ([d170b6b](https://github.com/topos-protocol/topos/commit/d170b6b386005d44457979bcd0a0f2435153f3f2))
- Adding storage context on startup ([#404](https://github.com/topos-protocol/topos/issues/404)) - ([ffae4c6](https://github.com/topos-protocol/topos/commit/ffae4c63d00bb099a49216c2af560f6b59601133))
- Remove audit ignore report ([#407](https://github.com/topos-protocol/topos/issues/407)) - ([70bce47](https://github.com/topos-protocol/topos/commit/70bce479d16c750ff1a322db1e88bfae08303855))
- Update SynchronizerService to remove unwrap ([#403](https://github.com/topos-protocol/topos/issues/403)) - ([b424aa9](https://github.com/topos-protocol/topos/commit/b424aa91f68197134faec742a23eec513cde837f))
- Adding no-color option to CLI ([#402](https://github.com/topos-protocol/topos/issues/402)) - ([4989936](https://github.com/topos-protocol/topos/commit/4989936ae000a85897d805453c91a53f4edd6580))
- Adding cross compilation ([#400](https://github.com/topos-protocol/topos/issues/400)) - ([887762f](https://github.com/topos-protocol/topos/commit/887762f740241d49d2886cd55d6a9da1d3d83e7e))
- Adding openssl as dependency ([#396](https://github.com/topos-protocol/topos/issues/396)) - ([60f873c](https://github.com/topos-protocol/topos/commit/60f873c619b8132a2276634205e3c39561eb3faf))
- Update release action target ([#395](https://github.com/topos-protocol/topos/issues/395)) - ([54db400](https://github.com/topos-protocol/topos/commit/54db40020e8e22d7b100b0b6944a45485e2939a4))
- Update release action target ([#394](https://github.com/topos-protocol/topos/issues/394)) - ([f0f28c3](https://github.com/topos-protocol/topos/commit/f0f28c33a5332232b2921b13051ae5de714cf58b))
- Adding aarch64 image ([#393](https://github.com/topos-protocol/topos/issues/393)) - ([9f48dc8](https://github.com/topos-protocol/topos/commit/9f48dc88582bfbc71cabb2c6bbc27297cc9b87ee))
- Cleanup topos test network setup ([#390](https://github.com/topos-protocol/topos/issues/390)) - ([2820664](https://github.com/topos-protocol/topos/commit/2820664a66bdfe039f1aaa80d79f4ff339c8a65c))


