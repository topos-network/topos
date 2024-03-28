import { isHexString, JsonRpcProvider, Wallet } from 'ethers'

import { TokenDeployer__factory } from '../typechain-types/factories/contracts/topos-core/TokenDeployer__factory'
import { ToposCore__factory } from '../typechain-types/factories/contracts/topos-core/ToposCore__factory'
import { ToposCoreProxy__factory } from '../typechain-types/factories/contracts/topos-core/ToposCoreProxy__factory'
import { ERC20Messaging__factory } from '../typechain-types/factories/contracts/examples/ERC20Messaging__factory'
import { ToposCore } from '../typechain-types/contracts/topos-core/ToposCore'

const main = async function (...args: string[]) {
  const [providerEndpoint, _sequencerPrivateKey] = args
  const provider = new JsonRpcProvider(providerEndpoint)
  const toposDeployerPrivateKey = sanitizeHexString(
    process.env.PRIVATE_KEY || ''
  )

  if (!_sequencerPrivateKey) {
    console.error('ERROR: Please provide the sequencer private key!')
    process.exit(1)
  }

  const sequencerPrivateKey = sanitizeHexString(_sequencerPrivateKey)

  if (!isHexString(sequencerPrivateKey, 32)) {
    console.error('ERROR: The sequencer private key is not a valid key!')
    process.exit(1)
  }

  const sequencerWallet = new Wallet(sequencerPrivateKey, provider)
  const sequencerPublicKey = sequencerWallet.signingKey.compressedPublicKey

  const subnetId = sanitizeHexString(sequencerPublicKey.substring(4))

  if (!isHexString(toposDeployerPrivateKey, 32)) {
    console.error(
      'ERROR: Please provide a valid toposDeployer private key! (PRIVATE_KEY)'
    )
    process.exit(1)
  }

  const toposDeployerWallet = new Wallet(toposDeployerPrivateKey, provider)

  // Deploy TokenDeployer
  const TokenDeployer = await new TokenDeployer__factory(
    toposDeployerWallet
  ).deploy({ gasLimit: 5_000_000 })
  await TokenDeployer.waitForDeployment()
  const tokenDeployerAddress = await TokenDeployer.getAddress()
  console.log(`Token Deployer deployed to ${tokenDeployerAddress}`)

  // Deploy ToposCore
  const ToposCore = await new ToposCore__factory(toposDeployerWallet).deploy({
    gasLimit: 5_000_000,
  })
  await ToposCore.waitForDeployment()
  const toposCoreAddress = await ToposCore.getAddress()
  console.log(`Topos Core contract deployed to ${toposCoreAddress}`)

  // Deploy ToposCoreProxy
  const ToposCoreProxy = await new ToposCoreProxy__factory(
    toposDeployerWallet
  ).deploy(toposCoreAddress, { gasLimit: 5_000_000 })
  await ToposCoreProxy.waitForDeployment()
  const toposCoreProxyAddress = await ToposCoreProxy.getAddress()
  console.log(`Topos Core Proxy contract deployed to ${toposCoreProxyAddress}`)

  console.info(`Initializing Topos Core Contract`)
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

  // Deploy ERC20Messaging
  const ERC20Messaging = await new ERC20Messaging__factory(
    toposDeployerWallet
  ).deploy(tokenDeployerAddress, toposCoreProxyAddress, { gasLimit: 5_000_000 })
  await ERC20Messaging.waitForDeployment()
  const erc20MessagingAddress = await ERC20Messaging.getAddress()
  console.log(`ERC20 Messaging contract deployed to ${erc20MessagingAddress}`)

  console.info(`Setting subnetId on ToposCore via proxy`)
  await toposCoreConnectedToSequencer
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

  console.info(`Reading subnet id`)
  const networkSubnetId = await toposCoreConnectedToSequencer.networkSubnetId()

  console.info(
    `Successfully set ${networkSubnetId} subnetId on ToposCore via proxy\n`
  )
}

const sanitizeHexString = function (hexString: string) {
  return hexString.startsWith('0x') ? hexString : `0x${hexString}`
}

async function initialize(
  toposCore: ToposCore,
  wallet: Wallet,
  adminThreshold: number
) {
  await toposCore
    .initialize([wallet.address], adminThreshold, {
      gasLimit: 4_000_000,
    })
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

const args = process.argv.slice(2)
main(...args)
