import { isHexString, JsonRpcProvider, Wallet } from 'ethers'
import { ERC20Messaging__factory } from '../../typechain-types/factories/contracts/examples/ERC20Messaging__factory'
import { BurnableMintableCappedERC20__factory } from '../../typechain-types/factories/contracts/topos-core/BurnableMintableCappedERC20__factory'
import { ERC20Messaging } from '../../typechain-types/contracts/examples/ERC20Messaging'
import * as cc from '../../test/topos-core/shared/constants/certificates'
import * as tc from '../../test/topos-core/shared/constants/tokens'
import * as testUtils from '../../test/topos-core/shared/utils/common'

const sanitizeHexString = (hexString: string) =>
  hexString.startsWith('0x') ? hexString : `0x${hexString}`

const checkAndDeployToken = async (
  erc20Messaging: ERC20Messaging,
  tokenSymbol: string,
  defaultTokenParams: string
) => {
  const numberOfTokens = await erc20Messaging.getTokenCount()
  for (let index = 0; index < numberOfTokens; index++) {
    const tokenKey = await erc20Messaging.getTokenKeyAtIndex(index)
    const [token, address] = await erc20Messaging.tokens(tokenKey)
    if (token === tokenSymbol) {
      return address
    }
  }

  const tokenDeployTx = await erc20Messaging.deployToken(defaultTokenParams, {
    gasLimit: 5_000_000,
  })
  await tokenDeployTx.wait()
  const token = await erc20Messaging.getTokenBySymbol(tokenSymbol)
  return token.addr
}

const main = async (...args: string[]) => {
  const [providerEndpoint, senderPrivateKey, receiverAddress, amount] = args
  const provider = new JsonRpcProvider(providerEndpoint)
  const erc20MessagingAddress = sanitizeHexString(
    process.env.ERC20_MESSAGING_ADDRESS || ''
  )
  const wallet = new Wallet(senderPrivateKey, provider)

  if (!isHexString(erc20MessagingAddress, 20))
    throw new Error('Invalid ERC20_MESSAGING_ADDRESS')

  const erc20Messaging = ERC20Messaging__factory.connect(
    erc20MessagingAddress,
    wallet
  )
  const defaultToken = testUtils.encodeTokenParam(
    tc.TOKEN_NAME,
    tc.TOKEN_SYMBOL_X,
    tc.MINT_CAP_100_000_000,
    tc.DAILY_MINT_LIMIT_100,
    tc.INITIAL_SUPPLY_10_000_000
  )
  const tokenAddress = await checkAndDeployToken(
    erc20Messaging,
    tc.TOKEN_SYMBOL_X,
    defaultToken
  )

  const erc20 = BurnableMintableCappedERC20__factory.connect(
    tokenAddress,
    wallet
  )
  console.log(`Balance before: ${await erc20.balanceOf(wallet.address)}`)
  const approveTx = await erc20.approve(erc20MessagingAddress, amount)
  await approveTx.wait()

  const sendTokenTx = await erc20Messaging.sendToken(
    cc.TARGET_SUBNET_ID_4,
    tc.TOKEN_SYMBOL_X,
    receiverAddress,
    BigInt(amount)
  )
  const txReceipt = await sendTokenTx.wait()

  if (txReceipt!.status === 1) {
    const remainingBalance = await erc20.balanceOf(wallet.address)
    console.log(`Token sent. Remaining balance: ${remainingBalance.toString()}`)
  } else {
    console.error('Sending token failed', txReceipt)
  }
}

const args = process.argv.slice(2)
main(...args).catch(console.error)
