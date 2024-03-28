import { isHexString, JsonRpcProvider, Wallet } from 'ethers'

import { ToposCore__factory } from '../typechain-types/factories/contracts/topos-core/ToposCore__factory'
import { ToposCoreProxy__factory } from '../typechain-types/factories/contracts/topos-core/ToposCoreProxy__factory'
import { ToposCore } from '../typechain-types/contracts/topos-core/ToposCore'

const main = async function (...args: string[]) {
  const [_providerEndpoint, _sequencerPrivateKey, _gasLimit] = args
  const provider = new JsonRpcProvider(_providerEndpoint)

  const toposDeployerPrivateKey = sanitizeHexString(
    process.env.PRIVATE_KEY || ''
  )
  if (!isHexString(toposDeployerPrivateKey, 32)) {
    console.error(
      'ERROR: Please provide a valid toposDeployer private key! (PRIVATE_KEY)'
    )
    process.exit(1)
  }

  const sequencerPrivateKey = sanitizeHexString(_sequencerPrivateKey || '')
  if (!isHexString(sequencerPrivateKey, 32)) {
    console.error('ERROR: Please provide a valid sequencer private key!')
    process.exit(1)
  }

  const toposDeployerWallet = new Wallet(toposDeployerPrivateKey, provider)

  const toposCore = await new ToposCore__factory(toposDeployerWallet).deploy({
    gasLimit: _gasLimit ? BigInt(_gasLimit) : 5_000_000,
  })
  await toposCore.waitForDeployment()
  const toposCoreAddress = await toposCore.getAddress()

  const toposCoreProxy = await new ToposCoreProxy__factory(
    toposDeployerWallet
  ).deploy(toposCoreAddress, {
    gasLimit: _gasLimit ? BigInt(_gasLimit) : 5_000_000,
  })
  await toposCoreProxy.waitForDeployment()
  const toposCoreProxyAddress = await toposCoreProxy.getAddress()

  const sequencerWallet = new Wallet(sequencerPrivateKey, provider)

  const toposCoreConnectedToSequencer = ToposCore__factory.connect(
    toposCoreProxyAddress,
    sequencerWallet
  )

  const adminThreshold = 1
  await initialize(
    toposCoreConnectedToSequencer,
    sequencerWallet,
    adminThreshold
  )

  const sequencerPublicKey = sequencerWallet.signingKey.compressedPublicKey
  const subnetId = sanitizeHexString(sequencerPublicKey.substring(4))
  await setSubnetId(toposCoreConnectedToSequencer, subnetId)

  console.log(`
export TOPOS_CORE_CONTRACT_ADDRESS=${toposCoreAddress}
export TOPOS_CORE_PROXY_CONTRACT_ADDRESS=${toposCoreProxyAddress}
  `)
}

async function initialize(
  toposCore: ToposCore,
  wallet: Wallet,
  adminThreshold: number
) {
  await toposCore
    .initialize([wallet.address], adminThreshold, { gasLimit: 4_000_000 })
    .then(async (tx) => {
      await tx.wait().catch((error) => {
        console.error(`Error: Failed (wait) to initialize ToposCore via proxy!`)
        console.error(error)
        process.exit(1)
      })
    })
    .catch((error: Error) => {
      console.error(`Error: Failed to initialize ToposCore via proxy!`)
      console.error(error)
      process.exit(1)
    })
}

const setSubnetId = async function (toposCore: ToposCore, subnetId: string) {
  await toposCore
    .setNetworkSubnetId(subnetId, { gasLimit: 4_000_000 })
    .then(async (tx) => {
      await tx.wait().catch((error) => {
        console.error(
          `Error: Failed (wait) to set ${subnetId} subnetId on ToposCore via proxy!`
        )
        console.error(error)
        process.exit(1)
      })
    })
    .catch((error: Error) => {
      console.error(
        `Error: Failed to set ${subnetId} subnetId on ToposCore via proxy!`
      )
      console.error(error)
      process.exit(1)
    })

  await toposCore.networkSubnetId()
}

const sanitizeHexString = function (hexString: string) {
  return hexString.startsWith('0x') ? hexString : `0x${hexString}`
}

const args = process.argv.slice(2)
main(...args)
