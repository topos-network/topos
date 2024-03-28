import { ethers } from 'hardhat'

function encodeCertParam(
  prevId: string,
  sourceSubnetId: string,
  stateRoot: string,
  txRoot: string,
  receiptRoot: string,
  targetSubnets: string[],
  verifier: number,
  certId: string,
  starkProof: string,
  signature: string
) {
  return ethers.AbiCoder.defaultAbiCoder().encode(
    [
      'bytes32',
      'bytes32',
      'bytes32',
      'bytes32',
      'bytes32',
      'bytes32[]',
      'uint32',
      'bytes32',
      'bytes',
      'bytes',
    ],
    [
      prevId,
      sourceSubnetId,
      stateRoot,
      txRoot,
      receiptRoot,
      targetSubnets,
      verifier,
      certId,
      starkProof,
      signature,
    ]
  )
}

function encodeTokenParam(
  tokenName: string,
  tokenSymbol: string,
  mintCap: number,
  dailyMintLimit: number,
  initialSupply: number
) {
  return ethers.AbiCoder.defaultAbiCoder().encode(
    ['string', 'string', 'uint256', 'uint256', 'uint256'],
    [tokenName, tokenSymbol, mintCap, dailyMintLimit, initialSupply]
  )
}

export { encodeCertParam, encodeTokenParam }
