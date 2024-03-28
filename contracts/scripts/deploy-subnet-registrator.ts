import { computeAddress, isHexString, JsonRpcProvider, Wallet } from 'ethers'

import subnetRegistratorJSON from '../artifacts/contracts/topos-core/SubnetRegistrator.sol/SubnetRegistrator.json'
import { SubnetRegistrator__factory } from '../typechain-types/factories/contracts/topos-core/SubnetRegistrator__factory'
import { Arg, deployContractConstant } from './const-addr-deployer'

const main = async function (..._args: Arg[]) {
  const [providerEndpoint, _adminPrivateKey, salt, gasLimit, ...args] = _args
  const provider = new JsonRpcProvider(<string>providerEndpoint)

  if (!_adminPrivateKey) {
    console.error('ERROR: Please provide the admin private key!')
    process.exit(1)
  }

  const adminPrivateKey = sanitizeHexString(_adminPrivateKey as string)
  if (!isHexString(adminPrivateKey, 32)) {
    console.error('ERROR: Please provide a valid private key!')
    process.exit(1)
  }

  const adminAddress = computeAddress(adminPrivateKey)

  // Fetch the deployer wallet
  const privateKey = process.env.PRIVATE_KEY
  if (!privateKey || !isHexString(privateKey, 32)) {
    console.error(
      'ERROR: Please provide a valid deployer private key! (PRIVATE_KEY)'
    )
    process.exit(1)
  }
  const deployerWallet = new Wallet(process.env.PRIVATE_KEY || '', provider)

  deployContractConstant(
    deployerWallet,
    subnetRegistratorJSON,
    salt as string,
    [...args],
    gasLimit as number
  )
    .then(async (contract) => {
      const address = await contract.getAddress()
      console.log(address)
      const subnetRegistrator = SubnetRegistrator__factory.connect(
        address,
        deployerWallet
      )

      subnetRegistrator.initialize(adminAddress)
    })
    .catch((error) => {
      console.error(error)
      process.exit(1)
    })
}

const sanitizeHexString = function (hexString: string) {
  return hexString.startsWith('0x') ? hexString : `0x${hexString}`
}

const args = process.argv.slice(2)
main(...args)
