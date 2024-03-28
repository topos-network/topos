import { Signer } from 'ethers'
import { ethers } from 'hardhat'
import { expect } from 'chai'

import { SubnetRegistrator__factory } from '../../typechain-types/factories/contracts/topos-core/SubnetRegistrator__factory'
import { SubnetRegistrator } from '../../typechain-types'

describe('SubnetRegistrator', () => {
  const chainId = 1
  const currencySymbol = 'SUB'
  const endpointHttp = 'http://127.0.0.1'
  const endpointWs = 'ws://127.0.0.1'
  const logoURL = 'http://image-url.com'
  const name = 'Test Subnet'
  const subnetId = ethers.encodeBytes32String('subnetId')

  async function deploySubnetRegistratorFixture() {
    const [admin, nonAdmin, toposDeployer] = await ethers.getSigners()
    const subnetRegistrator = await new SubnetRegistrator__factory(
      toposDeployer
    ).deploy()
    await subnetRegistrator.waitForDeployment()
    await subnetRegistrator.initialize(admin.address)
    return {
      admin,
      nonAdmin,
      subnetRegistrator,
      toposDeployer,
    }
  }

  describe('registerSubnet', () => {
    it('reverts if non-admin tries to register a subnet', async () => {
      const { nonAdmin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await expect(
        subnetRegistrator
          .connect(nonAdmin)
          .registerSubnet(
            chainId,
            currencySymbol,
            endpointHttp,
            endpointWs,
            logoURL,
            name,
            subnetId
          )
      ).to.be.revertedWith('Ownable: caller is not the owner')
    })

    it('reverts if the subnet is already registered', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      await expect(
        registerSubnet(
          admin,
          chainId,
          currencySymbol,
          endpointHttp,
          endpointWs,
          logoURL,
          name,
          subnetId,
          subnetRegistrator
        )
      ).to.be.revertedWith('Bytes32Set: key already exists in the set.')
    })

    it('registers a subnet', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      const subnet = await subnetRegistrator.subnets(subnetId)
      expect(subnet.name).to.equal(name)
      expect(subnet.currencySymbol).to.equal(currencySymbol)
      expect(subnet.endpointHttp).to.equal(endpointHttp)
      expect(subnet.endpointWs).to.equal(endpointWs)
      expect(subnet.logoURL).to.equal(logoURL)
      expect(subnet.chainId).to.equal(chainId)
    })

    it('gets the subnet count', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      const count = await subnetRegistrator.getSubnetCount()
      expect(count).to.equal(1)
    })

    it('gets the subnet at a given index', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      const id = await subnetRegistrator.getSubnetIdAtIndex(0)
      expect(id).to.equal(subnetId)
    })

    it('checks if a subnet exists', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      const exists = await subnetRegistrator.subnetExists(subnetId)
      expect(exists).to.be.true
    })

    it('emits a new subnet registered event', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await expect(
        registerSubnet(
          admin,
          chainId,
          currencySymbol,
          endpointHttp,
          endpointWs,
          logoURL,
          name,
          subnetId,
          subnetRegistrator
        )
      )
        .to.emit(subnetRegistrator, 'NewSubnetRegistered')
        .withArgs(subnetId)
    })
  })

  describe('removeSubnet', () => {
    it('reverts if non-admin tries to remove a subnet', async () => {
      const { nonAdmin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await expect(
        subnetRegistrator.connect(nonAdmin).removeSubnet(subnetId)
      ).to.be.revertedWith('Ownable: caller is not the owner')
    })

    it('reverts when removing a non-existent subnet', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await expect(
        removeSubnet(subnetId, subnetRegistrator, admin)
      ).to.be.revertedWith('Bytes32Set: key does not exist in the set.')
    })

    it('emit a subnet removed event', async () => {
      const { admin, subnetRegistrator } =
        await deploySubnetRegistratorFixture()
      await registerSubnet(
        admin,
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId,
        subnetRegistrator
      )
      await expect(removeSubnet(subnetId, subnetRegistrator, admin))
        .to.emit(subnetRegistrator, 'SubnetRemoved')
        .withArgs(subnetId)
    })
  })

  async function registerSubnet(
    admin: Signer,
    chainId: number,
    currencySymbol: string,
    endpointHttp: string,
    endpointWs: string,
    logoURL: string,
    name: string,
    subnetId: string,
    subnetRegistrator: SubnetRegistrator
  ) {
    return await subnetRegistrator
      .connect(admin)
      .registerSubnet(
        chainId,
        currencySymbol,
        endpointHttp,
        endpointWs,
        logoURL,
        name,
        subnetId
      )
  }

  async function removeSubnet(
    subnetId: string,
    subnetRegistrator: SubnetRegistrator,
    admin: Signer
  ) {
    return await subnetRegistrator.connect(admin).removeSubnet(subnetId)
  }
})
