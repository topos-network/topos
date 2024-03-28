import { EventLog } from 'ethers'
import { expect } from 'chai'
import { ethers } from 'hardhat'

import { BurnableMintableCappedERC20__factory } from '../../typechain-types/factories/contracts/topos-core/BurnableMintableCappedERC20__factory'
import { TokenDeployer__factory } from '../../typechain-types/factories/contracts/topos-core/TokenDeployer__factory'
import * as tc from './shared/constants/tokens'

describe('BurnableMintableCappedERC20', () => {
  async function deployTokenFixture() {
    const [deployer, operator, user1] = await ethers.getSigners()
    const tokenSalt = ethers.keccak256(
      Buffer.concat([
        Buffer.from(deployer.address),
        Buffer.from(tc.TOKEN_SYMBOL_X),
      ])
    )

    const tokenDeployer = await new TokenDeployer__factory(deployer).deploy()
    await tokenDeployer.waitForDeployment()

    const deployTokenTx = await tokenDeployer.deployToken(
      tc.TOKEN_NAME,
      tc.TOKEN_SYMBOL_X,
      tc.MINT_CAP_100_000_000,
      tc.INITIAL_SUPPLY_10_000_000,
      deployer.address,
      operator.address,
      tokenSalt,
      {}
    )

    const txReceipt = await deployTokenTx.wait()
    const log = txReceipt!.logs?.find(
      (l) => (l as EventLog).eventName === 'Deployed'
    )
    const tokenAddress = (log as EventLog).args['tokenAddress']
    const token = BurnableMintableCappedERC20__factory.connect(
      tokenAddress,
      deployer
    )

    return { deployer, operator, user1, token }
  }

  describe('Mint', () => {
    it('lets the deployer mint tokens', async () => {
      const { deployer, user1, token } = await deployTokenFixture()
      await token.connect(deployer).mint(user1.address, tc.MINT_AMOUNT_100)
      await expect(token.balanceOf(user1.address)).to.eventually.equal(
        tc.MINT_AMOUNT_100
      )
    })

    it('lets the operator mint tokens', async () => {
      const { operator, user1, token } = await deployTokenFixture()
      await token.connect(operator).mint(user1.address, tc.MINT_AMOUNT_100)
      await expect(token.balanceOf(user1.address)).to.eventually.equal(
        tc.MINT_AMOUNT_100
      )
    })

    it('does not let a non-operator or non-deployer mint tokens', async () => {
      const { user1, token } = await deployTokenFixture()
      await expect(
        token.connect(user1).mint(user1.address, tc.MINT_AMOUNT_100)
      ).to.be.revertedWith(
        'MintableCappedERC20: must have the admin or operator role to mint'
      )
    })
  })

  describe('Burn', () => {
    it('lets the deployer burn tokens', async () => {
      const { deployer, user1, token } = await deployTokenFixture()
      await token.connect(deployer).mint(user1.address, tc.MINT_AMOUNT_100)
      await token.connect(user1).approve(deployer.address, tc.SEND_AMOUNT_50)
      await token.connect(deployer).burnFrom(user1.address, tc.SEND_AMOUNT_50)
      await expect(token.balanceOf(user1.address)).to.eventually.equal(
        tc.SEND_AMOUNT_50
      )
    })

    it('lets the operator burn tokens', async () => {
      const { operator, user1, token } = await deployTokenFixture()
      await token.connect(operator).mint(user1.address, tc.MINT_AMOUNT_100)
      await token.connect(user1).approve(operator.address, tc.SEND_AMOUNT_50)
      await token.connect(operator).burnFrom(user1.address, tc.SEND_AMOUNT_50)
      await expect(token.balanceOf(user1.address)).to.eventually.equal(
        tc.SEND_AMOUNT_50
      )
    })

    it('does not let a non-operator or non-deployer burn tokens', async () => {
      const { operator, user1, token } = await deployTokenFixture()
      await token.connect(operator).mint(user1.address, tc.MINT_AMOUNT_100)
      await token.connect(user1).approve(operator.address, tc.SEND_AMOUNT_50)
      await expect(
        token.connect(user1).burnFrom(user1.address, tc.SEND_AMOUNT_50)
      ).to.be.revertedWith(
        'BurnableMintableCappedERC20: must have the admin or operator role to burn'
      )
    })
  })

  describe('Roles', () => {
    it('lets the deployer revoke the operator role', async () => {
      const { deployer, operator, user1, token } = await deployTokenFixture()
      await token
        .connect(deployer)
        .revokeRole(ethers.id('OPERATOR_ROLE'), operator.address)
      await expect(
        token.connect(operator).mint(user1.address, tc.MINT_AMOUNT_100)
      ).to.be.revertedWith(
        'MintableCappedERC20: must have the admin or operator role to mint'
      )
    })

    it('lets the deployer grant the operator role', async () => {
      const { deployer, operator, user1, token } = await deployTokenFixture()
      await token
        .connect(deployer)
        .revokeRole(ethers.id('OPERATOR_ROLE'), operator.address)
      await token
        .connect(deployer)
        .grantRole(ethers.id('OPERATOR_ROLE'), operator.address)
      await token.connect(operator).mint(user1.address, tc.MINT_AMOUNT_100)
      await expect(token.balanceOf(user1.address)).to.eventually.equal(
        tc.MINT_AMOUNT_100
      )
    })

    it('does not let a non-deployer grant the operator role', async () => {
      const { operator, user1, token } = await deployTokenFixture()
      await expect(
        token
          .connect(operator)
          .grantRole(ethers.id('OPERATOR_ROLE'), user1.address)
      ).to.be.reverted
    })

    it('does not let a non-deployer revoke the operator role', async () => {
      const { operator, user1, token } = await deployTokenFixture()
      await expect(
        token
          .connect(operator)
          .revokeRole(ethers.id('OPERATOR_ROLE'), user1.address)
      ).to.be.reverted
    })
  })
})
