import {
  AbstractProvider,
  BytesLike,
  ContractTransactionResponse,
  Interface,
  isHexString,
  JsonRpcProvider,
  Wallet,
} from 'ethers'

import { ToposCore__factory } from '../typechain-types/factories/contracts/topos-core/ToposCore__factory'
import { CodeHash__factory } from '../typechain-types/factories/contracts/topos-core/CodeHash__factory'

import toposCoreJSON from '../artifacts/contracts/topos-core/ToposCore.sol/ToposCore.json'

import {
  Arg,
  ContractOutputJSON,
  deployContractConstant,
  predictContractConstant,
} from './const-addr-deployer'

/**
 *
 * This script performs the following tasks:
 * 1. Deploys the `ToposCore` contract (using the `ConstAddressDeployer`)
 *    & upgrades it to the latest version
 * 2. Does not modify the `ToposCoreProxy` contract
 * 3. Does not deploy the `TokenDeployer` or `ERC20Messaging` contract
 */
const main = async function (...args: string[]) {
  const [
    _providerEndpoint,
    _sequencerPrivateKey,
    _toposCoreProxyAddress,
    _gasLimit,
  ] = args
  const provider = new JsonRpcProvider(_providerEndpoint)

  // this is used to deploy a new contract
  const toposDeployerPrivateKey = sanitizeHexString(
    process.env.PRIVATE_KEY || ''
  )
  if (!isHexString(toposDeployerPrivateKey, 32)) {
    console.error(
      'ERROR: Please provide a valid toposDeployer private key! (PRIVATE_KEY)'
    )
    process.exit(1)
  }

  // sequencerPrivateKey belongs to the owner of the ToposCore contract
  const sequencerPrivateKey = sanitizeHexString(_sequencerPrivateKey || '')
  if (!isHexString(sequencerPrivateKey, 32)) {
    console.error('ERROR: Please provide a valid sequencer private key!')
    process.exit(1)
  }

  const toposCoreSalt = process.env.TOPOS_CORE_SALT
  verifySalt('ToposCore', 'TOPOS_CORE_SALT', toposCoreSalt)

  const toposDeployerWallet = new Wallet(toposDeployerPrivateKey, provider)
  const sequencerWallet = new Wallet(sequencerPrivateKey, provider)

  const upgradedToposCoreAddress = await processContract(
    toposDeployerWallet,
    toposCoreJSON,
    toposCoreSalt!,
    [],
    4_000_000
  )

  const codeHash = await new CodeHash__factory(toposDeployerWallet).deploy()
  await codeHash.waitForDeployment()
  const toposCoreCodeHash = await codeHash.getCodeHash(upgradedToposCoreAddress)

  const toposCoreConnectedToSequencer = ToposCore__factory.connect(
    _toposCoreProxyAddress,
    sequencerWallet
  )
  const currentToposCoreAddress =
    await toposCoreConnectedToSequencer.implementation()
  console.log(`Current ToposCore address: ${currentToposCoreAddress}`)

  const upgradeTx = await toposCoreConnectedToSequencer.upgrade(
    upgradedToposCoreAddress,
    toposCoreCodeHash,
    {
      gasLimit: _gasLimit ? BigInt(_gasLimit) : 5_000_000,
    }
  )
  await processTransaction(provider, upgradeTx, [
    new Interface(toposCoreJSON.abi),
  ])
  console.log(
    `ToposCore successfully upgraded to ${await toposCoreConnectedToSequencer.implementation()}`
  )
}

const processContract = async function (
  toposDeployerWallet: Wallet,
  contractJson: ContractOutputJSON,
  salt: string,
  args: Arg[] = [],
  gasLimit: number | null = null
) {
  const predictedContractAddress = await predictContractConstant(
    toposDeployerWallet,
    contractJson,
    salt,
    args
  ).catch((error) => {
    console.error(error)
    process.exit(1)
  })

  const codeAtPredictedAddress = await toposDeployerWallet?.provider?.getCode(
    predictedContractAddress
  )

  const thereIsCodeAtAddress = codeAtPredictedAddress !== '0x'

  if (thereIsCodeAtAddress) {
    return predictedContractAddress
  } else {
    const newContractAddress = await deployContractConstant(
      toposDeployerWallet,
      contractJson,
      salt,
      args,
      gasLimit
    )
      .then(async (contract) => await contract.getAddress())
      .catch((error) => {
        console.error(error)
        process.exit(1)
      })

    return newContractAddress
  }
}

async function processTransaction(
  provider: AbstractProvider,
  inputTx: ContractTransactionResponse,
  interfaceList: Interface[]
) {
  try {
    const receipt = await inputTx.wait()
    if (receipt?.status === 1) {
      console.log(`Transaction executed successfully!`)
    }
  } catch (error) {
    let errorMessage
    const txResponse = await provider.getTransaction(inputTx.hash)
    if (txResponse === null) {
      errorMessage = `Transaction with hash ${inputTx.hash} not found.`
    } else {
      try {
        await provider.call(txResponse)
      } catch (customError: unknown) {
        if (
          typeof customError === 'object' &&
          customError !== null &&
          'data' in customError
        ) {
          for (const iface of interfaceList) {
            const data = customError.data as BytesLike
            const decodedCustomError = iface.parseError(data)
            if (decodedCustomError !== null) {
              errorMessage = `Transaction failed with custom error: ${
                decodedCustomError!.name
              } ${JSON.stringify({ args: decodedCustomError!.args })}`
            }
          }
        } else {
          errorMessage = (customError as Error).toString()
        }
      }
    }
    console.error(errorMessage)
    process.exit(1)
  }
}

const sanitizeHexString = function (hexString: string) {
  return hexString.startsWith('0x') ? hexString : `0x${hexString}`
}

const verifySalt = function (
  contractName: string,
  envVarName: string,
  localVar: string | undefined
) {
  if (!localVar) {
    console.error(
      `ERROR: Please provide a salt for ${contractName}! (${envVarName})`
    )
    process.exit(1)
  }
}

const args = process.argv.slice(2)
main(...args)
