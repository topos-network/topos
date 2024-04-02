# topos-tce

This library is the entry point for the TCE node. It is responsible for setting up the
different components of the TCE node and starting them.

The TCE node is composed of the following components:
- P2P network: [topos_p2p]
- Reliable Broadcast: [topos_tce_broadcast]
- Synchronizer: [topos_tce_synchronizer]
- Storage: [topos_tce_storage]
- APIs: [topos_tce_api]
- Gatekeeper: [topos_tce_gatekeeper]

This library exposes a single function `launch` that takes a [TceConfig] and a [CancellationToken]
and returns a [Future] that resolves to an [ExitStatus] when the TCE node is shut down.

### Interactions

The `topos_tce` crate is responsible for connecting all the different components of the TCE node
together. Different flow are managed by the `AppContext` struct:

<picture>
 <source media="(prefers-color-scheme: dark)" srcset="https://github.com/topos-protocol/topos/assets/1394604/02e7f208-e6af-4280-85e3-5e1df8506bd4">
 <img alt="Text changing depending on mode. Light: 'So light!' Dark: 'So dark!'" src="https://github.com/topos-protocol/topos/assets/1394604/70f9a3f8-bd52-4856-bf62-d5ed4b70ff09">
</picture>

##### P2P layer

After setting up the P2P layer, the `AppContext` will listen for incoming events and dispatch
them to the different components of the TCE node.

The [AppContext] is listening for [topos_p2p::Event] on a channel. Based on those events the
[AppContext] will decide to forward them to the [topos_tce_broadcast] after checking for state
in the [topos_tce_storage].

The [AppContext] will also send message to [topos_p2p] when the [topos_tce_broadcast] is
producing events, those messages are published on the network to support the Topos Protocol.


