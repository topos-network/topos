import {
  AbiCoder,
  Contract,
  ContractFactory,
  HDNodeWallet,
  InterfaceAbi,
  keccak256,
  Wallet,
} from 'ethers'

import { ConstAddressDeployer__factory } from '../typechain-types/factories/contracts/topos-core/ConstAddressDeployer__factory'

export type Arg = string | number

export type ContractOutputJSON = { abi: InterfaceAbi; bytecode: string }

const CONST_ADDRESS_DEPLOYER_ADDR = '0x0000000000000000000000000000000000001110'

const getSaltFromKey = (key: string) => {
  const coder = AbiCoder.defaultAbiCoder()
  return keccak256(coder.encode(['string'], [key.toString()]))
}

export const estimateGasForDeploy = async (
  wallet: Wallet | HDNodeWallet,
  contractJson: ContractOutputJSON,
  args: Arg[] = []
) => {
  const deployer = ConstAddressDeployer__factory.connect(
    CONST_ADDRESS_DEPLOYER_ADDR,
    wallet
  )

  const salt = getSaltFromKey('')
  const factory = new ContractFactory(contractJson.abi, contractJson.bytecode)
  const bytecode = (await factory.getDeployTransaction(...args)).data
  return await deployer.deploy.estimateGas(bytecode, salt)
}

export const estimateGasForDeployAndInit = async (
  wallet: Wallet,
  contractJson: ContractOutputJSON,
  args: Arg[] = [],
  initArgs: Arg[] = []
) => {
  const deployer = ConstAddressDeployer__factory.connect(
    CONST_ADDRESS_DEPLOYER_ADDR,
    wallet
  )

  const salt = getSaltFromKey('')
  const factory = new ContractFactory(contractJson.abi, contractJson.bytecode)
  const bytecode = (await factory.getDeployTransaction(...args)).data

  const address = await deployer.deployedAddress(bytecode, wallet.address, salt)
  const contract = new Contract(address, contractJson.abi, wallet)
  const initData = (await contract.init.populateTransaction(...initArgs)).data

  return deployer.deployAndInit.estimateGas(bytecode, salt, initData)
}

export const deployContractConstant = async (
  wallet: Wallet | HDNodeWallet,
  contractJson: ContractOutputJSON,
  key: string,
  args: Arg[] = [],
  gasLimit: number | null = null,
  constAddrDeployerAddress?: string
) => {
  const deployer = ConstAddressDeployer__factory.connect(
    constAddrDeployerAddress
      ? constAddrDeployerAddress
      : CONST_ADDRESS_DEPLOYER_ADDR,
    wallet
  )

  const salt = getSaltFromKey(key)

  const factory = new ContractFactory(contractJson.abi, contractJson.bytecode)

  const bytecode = (await factory.getDeployTransaction(...args)).data

  const gas = gasLimit
    ? BigInt(gasLimit)
    : await estimateGasForDeploy(wallet, contractJson, args)

  const tx = await deployer.deploy(bytecode, salt, {
    gasLimit: BigInt(Math.floor(Number(gas) * 1.2)),
  })
  await tx.wait()

  const address = await deployer.deployedAddress(bytecode, wallet.address, salt)

  return new Contract(address, contractJson.abi, wallet)
}

export const deployAndInitContractConstant = async (
  wallet: Wallet,
  contractJson: ContractOutputJSON,
  key: string,
  args: Arg[] = [],
  initArgs: Arg[] = [],
  gasLimit: number | null = null
) => {
  const deployer = ConstAddressDeployer__factory.connect(
    CONST_ADDRESS_DEPLOYER_ADDR,
    wallet
  )
  const salt = getSaltFromKey(key)
  const factory = new ContractFactory(contractJson.abi, contractJson.bytecode)
  const bytecode = (await factory.getDeployTransaction(...args)).data
  const address = await deployer.deployedAddress(bytecode, wallet.address, salt)
  const contract = new Contract(address, contractJson.abi, wallet)
  const initData = (await contract.init.populateTransaction(...initArgs)).data
  const tx = await deployer.deployAndInit(bytecode, salt, initData, {
    gasLimit,
  })
  await tx.wait()
  return contract
}

export const predictContractConstant = async (
  wallet: Wallet,
  contractJson: ContractOutputJSON,
  key: string,
  args: Arg[] = []
) => {
  const deployer = ConstAddressDeployer__factory.connect(
    CONST_ADDRESS_DEPLOYER_ADDR,
    wallet
  )
  const salt = getSaltFromKey(key)

  const factory = new ContractFactory(contractJson.abi, contractJson.bytecode)
  const bytecode = (await factory.getDeployTransaction(...args)).data
  return await deployer.deployedAddress(bytecode, wallet.address, salt)
}
