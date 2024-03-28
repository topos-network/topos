import {
  loadFixture,
  takeSnapshot,
} from '@nomicfoundation/hardhat-network-helpers'
import { expect } from 'chai'
import { EventLog, keccak256, Provider, Signer, Wallet } from 'ethers'
import { ethers, network } from 'hardhat'

import * as tokenDeployerJSON from '../../artifacts/contracts/topos-core/TokenDeployer.sol/TokenDeployer.json'
import { deployContractConstant } from '../../scripts/const-addr-deployer'
import {
  ConstAddressDeployer__factory,
  ERC20__factory,
  ToposCore__factory,
  ToposCoreProxy__factory,
  ERC20Messaging__factory,
  ERC20Messaging,
} from '../../typechain-types'
import * as cc from './shared/constants/certificates'
import * as testUtils from './shared/utils/common'
import { getReceiptMptProof } from './shared/utils/mpt_proof'
import * as tc from './shared/constants/tokens'
import * as txc from './shared/constants/transactions'

describe('ToposMessaging', () => {
  async function deployERC20MessagingFixture() {
    await network.provider.send('hardhat_reset')
    const defaultAddressMnemonic =
      'test test test test test test test test test test test junk'

    const tokenDeployerSalt = keccak256(Buffer.from('TokenDeployer'))
    const [admin, receiver] = await ethers.getSigners()
    let wallet = Wallet.fromPhrase(defaultAddressMnemonic)
    wallet = wallet.connect(ethers.provider)

    const defaultCert = testUtils.encodeCertParam(
      cc.PREV_CERT_ID_0,
      cc.SOURCE_SUBNET_ID_1,
      cc.STATE_ROOT_MAX,
      cc.TX_ROOT_MAX,
      cc.RECEIPT_ROOT_MAX,
      [cc.TARGET_SUBNET_ID_4, cc.TARGET_SUBNET_ID_5],
      cc.VERIFIER,
      cc.CERT_ID_1,
      cc.DUMMY_STARK_PROOF,
      cc.DUMMY_SIGNATURE
    )
    const defaultToken = testUtils.encodeTokenParam(
      tc.TOKEN_NAME,
      tc.TOKEN_SYMBOL_X,
      tc.MINT_CAP_100_000_000,
      tc.DAILY_MINT_LIMIT_100,
      tc.INITIAL_SUPPLY_10_000_000
    )
    const adminAddresses = [admin.address]
    const adminThreshold = 1

    const constAddressDeployer = await new ConstAddressDeployer__factory(
      admin
    ).deploy()
    await constAddressDeployer.waitForDeployment()
    const constAddressDeployerAddress = await constAddressDeployer.getAddress()

    const tokenDeployer = await deployContractConstant(
      wallet,
      tokenDeployerJSON,
      tokenDeployerSalt,
      [],
      4_000_000,
      constAddressDeployerAddress
    )
    const tokenDeployerAddress = await tokenDeployer.getAddress()

    const toposCoreImplementation = await new ToposCore__factory(admin).deploy()
    await toposCoreImplementation.waitForDeployment()
    const toposCoreImplementationAddress =
      await toposCoreImplementation.getAddress()

    const toposCoreProxy = await new ToposCoreProxy__factory(admin).deploy(
      toposCoreImplementationAddress
    )
    await toposCoreProxy.waitForDeployment()
    const toposCoreProxyAddress = await toposCoreProxy.getAddress()

    const toposCore = ToposCore__factory.connect(toposCoreProxyAddress, admin)
    await toposCore.initialize(adminAddresses, adminThreshold)

    const erc20Messaging = await new ERC20Messaging__factory(admin).deploy(
      tokenDeployerAddress,
      toposCoreProxyAddress
    )
    await erc20Messaging.waitForDeployment()
    const erc20MessagingAddress = await erc20Messaging.getAddress()

    const erc20Messaging2 = await new ERC20Messaging__factory(admin).deploy(
      tokenDeployerAddress,
      toposCoreProxyAddress
    )
    await erc20Messaging2.waitForDeployment()

    return {
      admin,
      receiver,
      defaultCert,
      defaultToken,
      toposCore,
      erc20Messaging,
      erc20Messaging2,
      erc20MessagingAddress,
    }
  }

  describe('deployToken', () => {
    it('gets the token count', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await erc20Messaging.deployToken(defaultToken)
      expect(await erc20Messaging.getTokenCount()).to.equal(1)
    })

    it('gets count for multiple tokens', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await erc20Messaging.deployToken(defaultToken)
      const tokenTwo = testUtils.encodeTokenParam(
        tc.TOKEN_NAME,
        tc.TOKEN_SYMBOL_Y,
        tc.MINT_CAP_100_000_000,
        tc.DAILY_MINT_LIMIT_100,
        tc.INITIAL_SUPPLY_10_000_000
      )
      await erc20Messaging.deployToken(tokenTwo)
      expect(await erc20Messaging.getTokenCount()).to.equal(2)
    })

    it('gets token by token key hash', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      const tx = await erc20Messaging.deployToken(defaultToken)
      const txReceipt = await tx.wait()
      const log = txReceipt!.logs.find(
        (l) => (l as EventLog).eventName === 'TokenDeployed'
      ) as EventLog
      const tokenAddress = log.args.tokenAddress
      const tokenKeyHash = await erc20Messaging.getTokenKeyAtIndex(0)
      const token = await erc20Messaging.tokens(tokenKeyHash)
      expect(token[0]).to.equal(tc.TOKEN_SYMBOL_X)
      expect(token[1]).to.equal(tokenAddress)
    })

    it('reverts if a deployer deploys a token with the same symbol twice', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await erc20Messaging.deployToken(defaultToken)
      await expect(erc20Messaging.deployToken(defaultToken)).to.be.reverted
      expect(await erc20Messaging.getTokenCount()).to.equal(1)
    })

    it('reverts if two deployers deploy a token with the same symbol', async () => {
      const [, deployer2] = await ethers.getSigners()
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await erc20Messaging.deployToken(defaultToken) // deployed by admin
      const erc20Messaging2 = erc20Messaging.connect(deployer2)
      await expect(erc20Messaging2.connect(deployer2).deployToken(defaultToken))
        .to.be.reverted
      expect(await erc20Messaging.getTokenCount()).to.equal(1)
    })

    it('emits a token deployed event', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      const tx = await erc20Messaging.deployToken(defaultToken)
      const txReceipt = await tx.wait()
      const log = txReceipt!.logs?.find(
        (l) => (l as EventLog).eventName === 'TokenDeployed'
      ) as EventLog
      const tokenAddress = log.args.tokenAddress
      await expect(tx)
        .to.emit(erc20Messaging, 'TokenDeployed')
        .withArgs(tc.TOKEN_SYMBOL_X, tokenAddress)
    })
  })

  describe('execute', () => {
    it('deploys a token with zero mint limit', async () => {
      const {
        admin,
        receiver,
        toposCore,
        erc20Messaging,
        erc20MessagingAddress,
      } = await loadFixture(deployERC20MessagingFixture)
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const token = testUtils.encodeTokenParam(
        tc.TOKEN_NAME,
        tc.TOKEN_SYMBOL_X,
        tc.MINT_CAP_100_000_000,
        0, // zero mint limit
        tc.INITIAL_SUPPLY_10_000_000
      )
      const tx = await erc20Messaging.deployToken(token)
      const txReceipt = await tx.wait()
      const log = txReceipt!.logs?.find(
        (l) => (l as EventLog).eventName === 'TokenDeployed'
      ) as EventLog
      const tokenAddress = log.args.tokenAddress
      const erc20 = ERC20__factory.connect(tokenAddress, admin)
      await erc20.approve(erc20MessagingAddress, tc.SEND_AMOUNT_50)

      const sendToken = await sendTokenTx(
        erc20Messaging,
        ethers.provider,
        receiver.address,
        admin,
        cc.SOURCE_SUBNET_ID_2,
        tc.TOKEN_SYMBOL_X,
        tc.SEND_AMOUNT_50
      )

      const { proofBlob, receiptsRoot } = await getReceiptMptProof(
        sendToken,
        ethers.provider
      )

      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      )
        .to.emit(erc20, 'Transfer')
        .withArgs(ethers.ZeroAddress, receiver.address, tc.SEND_AMOUNT_50)
    })

    it('reverts if the log index is out of range', async () => {
      const { admin, receiver, defaultToken, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )
      await expect(
        erc20Messaging.execute([], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'LogIndexOutOfRange')
    })

    it('reverts if the certificate is not present', async () => {
      const { admin, receiver, defaultToken, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )

      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'CertNotPresent')
    })

    it('reverts if the merkle proof is invalid', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )

      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      const fakeProofBlob = '0x01'
      await expect(
        erc20Messaging.execute(
          [tc.TOKEN_SENT_INDEX_2],
          fakeProofBlob,
          receiptsRoot
        )
      ).to.be.reverted
    })

    it('reverts if the transaction is already executed', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )

      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await erc20Messaging.execute(
        [tc.TOKEN_SENT_INDEX_2],
        proofBlob,
        receiptsRoot
      )
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(
        erc20Messaging,
        'TransactionAlreadyExecuted'
      )
    })

    it('reverts if the transaction status is invalid', async () => {
      const { toposCore, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        txc.INVALID_STATUS_TRANSACTION.receiptRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute(
          [tc.TOKEN_SENT_INDEX_2],
          txc.INVALID_STATUS_TRANSACTION.proofBlob,
          txc.INVALID_STATUS_TRANSACTION.receiptRoot
        )
      ).to.be.revertedWithCustomError(
        erc20Messaging,
        'InvalidTransactionStatus'
      )
    })

    it('reverts if the tx is not coming from the same sending contract', async () => {
      const {
        admin,
        receiver,
        defaultToken,
        toposCore,
        erc20Messaging,
        erc20Messaging2, // different sending contract
      } = await loadFixture(deployERC20MessagingFixture)
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging2
      )

      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'InvalidOriginAddress')
    })

    it('reverts if the target subnet id is mismatched', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )

      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_1) // target subnet id = SOURCE_SUBNET_ID_2
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'InvalidSubnetId')
    })

    it('reverts if the token is not deployed yet', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)

      const snapshot = await takeSnapshot()
      const { proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )
      await snapshot.restore()

      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'TokenDoesNotExist')
    })

    it('reverts if the daily mint limit is exceeded', async () => {
      const {
        admin,
        receiver,
        toposCore,
        erc20Messaging,
        erc20MessagingAddress,
      } = await loadFixture(deployERC20MessagingFixture)
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const smallDailyMintLimit = 1
      const token = testUtils.encodeTokenParam(
        tc.TOKEN_NAME,
        tc.TOKEN_SYMBOL_X,
        tc.MINT_CAP_100_000_000,
        smallDailyMintLimit,
        tc.INITIAL_SUPPLY_10_000_000
      )
      await erc20Messaging.deployToken(token)
      const deployedToken = await erc20Messaging.getTokenBySymbol(
        tc.TOKEN_SYMBOL_X
      )
      const erc20 = ERC20__factory.connect(deployedToken.addr, admin)
      await erc20.approve(erc20MessagingAddress, tc.SEND_AMOUNT_50)

      const sendToken = await sendTokenTx(
        erc20Messaging,
        ethers.provider,
        receiver.address,
        admin,
        cc.SOURCE_SUBNET_ID_2,
        tc.TOKEN_SYMBOL_X,
        tc.SEND_AMOUNT_50
      )

      const { proofBlob, receiptsRoot } = await getReceiptMptProof(
        sendToken,
        ethers.provider
      )
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWithCustomError(erc20Messaging, 'ExceedDailyMintLimit')
    })

    it('reverts if trying to mint for zero address', async () => {
      const { admin, toposCore, erc20Messaging, erc20MessagingAddress } =
        await loadFixture(deployERC20MessagingFixture)
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const token = testUtils.encodeTokenParam(
        tc.TOKEN_NAME,
        tc.TOKEN_SYMBOL_X,
        tc.MINT_CAP_100_000_000,
        tc.DAILY_MINT_LIMIT_100,
        tc.INITIAL_SUPPLY_10_000_000
      )
      await erc20Messaging.deployToken(token)
      const deployedToken = await erc20Messaging.getTokenBySymbol(
        tc.TOKEN_SYMBOL_X
      )
      const erc20 = ERC20__factory.connect(deployedToken.addr, admin)
      await erc20.approve(erc20MessagingAddress, tc.SEND_AMOUNT_50)

      const sendToken = await sendTokenTx(
        erc20Messaging,
        ethers.provider,
        ethers.ZeroAddress, // sending to a zero address
        admin,
        cc.SOURCE_SUBNET_ID_2,
        tc.TOKEN_SYMBOL_X,
        tc.SEND_AMOUNT_50
      )

      const { proofBlob, receiptsRoot } = await getReceiptMptProof(
        sendToken,
        ethers.provider
      )

      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      ).to.be.revertedWith('ERC20: mint to the zero address')
    })

    it('can execute a transaction with same inputs twice', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      // first transaction
      const { erc20, proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      )
        .to.emit(erc20, 'Transfer')
        .withArgs(ethers.ZeroAddress, receiver.address, tc.SEND_AMOUNT_50)
      await expect(erc20.balanceOf(receiver.address)).to.eventually.equal(
        tc.SEND_AMOUNT_50
      )

      // second transaction
      await erc20.approve(await erc20Messaging.getAddress(), tc.SEND_AMOUNT_50)
      const sendToken = await sendTokenTx(
        erc20Messaging,
        ethers.provider,
        await receiver.getAddress(),
        admin,
        cc.SOURCE_SUBNET_ID_2,
        await erc20.symbol(),
        tc.SEND_AMOUNT_50
      )

      const { proofBlob: proofBlob2, receiptsRoot: receiptsRoot2 } =
        await getReceiptMptProof(sendToken, ethers.provider)
      const certificate2 = testUtils.encodeCertParam(
        cc.CERT_ID_1,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot2,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_2,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate2, cc.CERT_POS_2)
      await expect(
        erc20Messaging.execute(
          [tc.TOKEN_SENT_INDEX_2],
          proofBlob2,
          receiptsRoot2
        )
      )
        .to.emit(erc20, 'Transfer')
        .withArgs(ethers.ZeroAddress, receiver.address, tc.SEND_AMOUNT_50)
      await expect(erc20.balanceOf(receiver.address)).to.eventually.equal(
        tc.SEND_AMOUNT_50 * 2
      )
    })

    it('emits the Transfer success event', async () => {
      const { admin, receiver, defaultToken, toposCore, erc20Messaging } =
        await loadFixture(deployERC20MessagingFixture)
      const { erc20, proofBlob, receiptsRoot } = await deployDefaultToken(
        admin,
        receiver,
        defaultToken,
        erc20Messaging
      )
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const certificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        cc.SOURCE_SUBNET_ID_1,
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        receiptsRoot,
        [cc.SOURCE_SUBNET_ID_2],
        cc.VERIFIER,
        cc.CERT_ID_1,
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(certificate, cc.CERT_POS_1)
      await expect(
        erc20Messaging.execute([tc.TOKEN_SENT_INDEX_2], proofBlob, receiptsRoot)
      )
        .to.emit(erc20, 'Transfer')
        .withArgs(ethers.ZeroAddress, receiver.address, tc.SEND_AMOUNT_50)
      await expect(erc20.balanceOf(receiver.address)).to.eventually.equal(
        tc.SEND_AMOUNT_50
      )
    })
  })

  describe('sendToken', () => {
    it('reverts if the token is not deployed yet', async () => {
      const [token] = await ethers.getSigners()
      const { erc20Messaging } = await loadFixture(deployERC20MessagingFixture)
      await expect(
        erc20Messaging.sendToken(
          cc.TARGET_SUBNET_ID_4,
          token.address,
          tc.RECIPIENT_ADDRESS,
          tc.SEND_AMOUNT_50
        )
      )
        .to.be.revertedWithCustomError(erc20Messaging, 'TokenDoesNotExist')
        .withArgs(token.address)
    })

    it('reverts if the send amount is zero', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )
      await erc20Messaging.deployToken(defaultToken)
      await expect(
        erc20Messaging.sendToken(
          cc.TARGET_SUBNET_ID_4,
          tc.TOKEN_SYMBOL_X,
          tc.RECIPIENT_ADDRESS,
          0
        )
      ).to.be.revertedWithCustomError(erc20Messaging, 'InvalidAmount')
    })

    it('reverts if the send amount is not approved', async () => {
      const { defaultToken, erc20Messaging } = await loadFixture(
        deployERC20MessagingFixture
      )

      await erc20Messaging.deployToken(defaultToken)
      await expect(
        erc20Messaging.sendToken(
          cc.TARGET_SUBNET_ID_4,
          tc.TOKEN_SYMBOL_X,
          tc.RECIPIENT_ADDRESS,
          tc.SEND_AMOUNT_50
        )
      )
        .to.be.revertedWithCustomError(erc20Messaging, 'BurnFailed')
        .withArgs(tc.TOKEN_SYMBOL_X)
    })

    it('emits a token sent event', async () => {
      const {
        admin,
        defaultToken,
        toposCore,
        erc20Messaging,
        erc20MessagingAddress,
      } = await loadFixture(deployERC20MessagingFixture)
      await toposCore.setNetworkSubnetId(cc.SOURCE_SUBNET_ID_2)
      const tx = await erc20Messaging.deployToken(defaultToken)
      const txReceipt = await tx.wait()
      const log = txReceipt!.logs?.find(
        (l) => (l as EventLog).eventName === 'TokenDeployed'
      ) as EventLog
      const tokenAddress = log.args.tokenAddress
      const erc20 = ERC20__factory.connect(tokenAddress, admin)
      await erc20.approve(erc20MessagingAddress, tc.SEND_AMOUNT_50)

      await expect(
        erc20Messaging.sendToken(
          cc.TARGET_SUBNET_ID_4,
          tc.TOKEN_SYMBOL_X,
          tc.RECIPIENT_ADDRESS,
          tc.SEND_AMOUNT_50
        )
      )
        .to.emit(erc20, 'Transfer')
        .withArgs(admin.address, ethers.ZeroAddress, tc.SEND_AMOUNT_50)
        .to.emit(erc20Messaging, 'TokenSent')
        .withArgs(
          cc.TARGET_SUBNET_ID_4,
          tc.TOKEN_SYMBOL_X,
          tokenAddress,
          tc.RECIPIENT_ADDRESS,
          tc.SEND_AMOUNT_50
        )
        .to.emit(toposCore, 'CrossSubnetMessageSent')
        .withArgs(cc.TARGET_SUBNET_ID_4, cc.SOURCE_SUBNET_ID_2, 1)
    })
  })

  async function deployDefaultToken(
    admin: Signer,
    receiver: Signer,
    defaultToken: string,
    erc20Messaging: ERC20Messaging
  ) {
    const erc20MessagingAddress = await erc20Messaging.getAddress()
    const tx = await erc20Messaging.deployToken(defaultToken)
    const txReceipt = await tx.wait()
    const log = txReceipt!.logs?.find(
      (l) => (l as EventLog).eventName === 'TokenDeployed'
    ) as EventLog
    const tokenAddress = log.args.tokenAddress
    const erc20 = ERC20__factory.connect(tokenAddress, admin)
    await erc20.approve(erc20MessagingAddress, tc.SEND_AMOUNT_50)

    const receiverAddress = await receiver.getAddress()

    const sendToken = await sendTokenTx(
      erc20Messaging,
      ethers.provider,
      receiverAddress,
      admin,
      cc.SOURCE_SUBNET_ID_2,
      await erc20.symbol(),
      tc.SEND_AMOUNT_50
    )

    const { proofBlob, receiptsRoot } = await getReceiptMptProof(
      sendToken,
      ethers.provider
    )
    return { erc20, proofBlob, receiptsRoot }
  }

  async function sendTokenTx(
    erc20Messaging: ERC20Messaging,
    provider: Provider,
    receiver: string,
    signer: Signer,
    targetSubnetId: string,
    symbol: string,
    amount: number
  ) {
    const estimatedGasLimit = await erc20Messaging.sendToken.estimateGas(
      targetSubnetId,
      symbol,
      receiver,
      amount,
      { gasLimit: 4_000_000 }
    )
    const TxUnsigned = await erc20Messaging.sendToken.populateTransaction(
      targetSubnetId,
      symbol,
      receiver,
      amount,
      { gasLimit: 4_000_000 }
    )
    TxUnsigned.chainId = BigInt(31337) // Hardcoded chainId for Hardhat local testing
    TxUnsigned.gasLimit = estimatedGasLimit
    const address = await signer.getAddress()
    const nonce = await provider.getTransactionCount(address)
    const feeData = await provider.getFeeData()
    TxUnsigned.nonce = nonce
    TxUnsigned.gasPrice = BigInt(feeData.gasPrice!)

    const submittedTx = await signer.sendTransaction(TxUnsigned)
    const receipt = await submittedTx.wait()
    if (receipt!.status === 0) {
      throw new Error(
        `Send Token Tx is reverted with Tx hash : ${submittedTx.hash}`
      )
    }
    return submittedTx
  }
})
