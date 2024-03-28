import { loadFixture } from '@nomicfoundation/hardhat-network-helpers'
import { ethers } from 'hardhat'
import { expect } from 'chai'

import { ToposCore__factory } from '../../typechain-types/factories/contracts/topos-core/ToposCore__factory'
import { ToposCoreProxy__factory } from '../../typechain-types/factories/contracts/topos-core/ToposCoreProxy__factory'
import { CodeHash__factory } from '../../typechain-types/factories/contracts/topos-core/CodeHash__factory'
import { ToposCoreProxy } from '../../typechain-types/contracts/topos-core/ToposCoreProxy'
import * as cc from './shared/constants/certificates'
import * as testUtils from './shared/utils/common'

describe('ToposCore', () => {
  async function deployToposCoreFixture() {
    const [admin] = await ethers.getSigners()
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
    const adminAddresses = [admin.address]
    const adminThreshold = 1

    const toposCoreImplementation = await new ToposCore__factory(admin).deploy()
    await toposCoreImplementation.waitForDeployment()
    const toposCoreImplementationAddress =
      await toposCoreImplementation.getAddress()

    const toposCoreProxy = await new ToposCoreProxy__factory(admin).deploy(
      toposCoreImplementationAddress
    )
    const toposCoreProxyAddress = await toposCoreProxy.getAddress()

    const toposCore = ToposCore__factory.connect(toposCoreProxyAddress, admin)
    await toposCore.initialize(adminAddresses, adminThreshold)

    const altToposCoreImplementation = await new ToposCore__factory(
      admin
    ).deploy()
    await altToposCoreImplementation.waitForDeployment()
    const altToposCoreImplementationAddress =
      await altToposCoreImplementation.getAddress()

    const altToposCoreProxy = await new ToposCoreProxy__factory(admin).deploy(
      altToposCoreImplementationAddress
    )
    await altToposCoreProxy.waitForDeployment()
    const altToposCoreProxyAddress = await altToposCoreProxy.getAddress()
    const altToposCore = ToposCore__factory.connect(
      altToposCoreProxyAddress,
      admin
    )
    const altToposCoreAddress = await altToposCore.getAddress()

    return {
      altToposCore,
      altToposCoreAddress,
      altToposCoreImplementation,
      altToposCoreImplementationAddress,
      admin,
      adminAddresses,
      adminThreshold,
      defaultCert,
      toposCore,
      toposCoreProxy,
      toposCoreProxyAddress,
      toposCoreImplementation,
    }
  }

  describe('initialize', () => {
    it('reverts if implementation contract has already been initialized', async () => {
      const { adminAddresses, adminThreshold, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      await expect(
        toposCore.initialize(adminAddresses, adminThreshold)
      ).to.be.revertedWith('Initializable: contract is already initialized')
    })

    it('reverts if the admin threshold mismatch the length of the admin list', async () => {
      const { adminAddresses, altToposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const falseAdminThreshold = 2 // admin threshold is 2, but we supply one admin address
      await expect(
        altToposCore.initialize(adminAddresses, falseAdminThreshold)
      ).to.be.revertedWithCustomError(altToposCore, 'InvalidAdmins')
    })

    it('reverts if the admin threshold is zero', async () => {
      const { adminAddresses, altToposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const falseAdminThreshold = 0 // admin threshold is 0, but we supply one admin address
      await expect(
        altToposCore.initialize(adminAddresses, falseAdminThreshold)
      ).to.be.revertedWithCustomError(altToposCore, 'InvalidAdminThreshold')
    })

    it('reverts if trying to add duplicate admins', async () => {
      const { admin, adminThreshold, altToposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const adminAddresses = [admin.address, admin.address] // duplicate admins
      await expect(
        altToposCore.initialize(adminAddresses, adminThreshold)
      ).to.to.be.revertedWithCustomError(altToposCore, 'DuplicateAdmin')
    })

    it('reverts if the admin address is zero address', async () => {
      const { adminThreshold, altToposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const adminAddresses = [ethers.ZeroAddress] // zero address admin
      await expect(
        altToposCore.initialize(adminAddresses, adminThreshold)
      ).to.to.be.revertedWithCustomError(altToposCore, 'InvalidAdmins')
    })
  })

  describe('pushCertificate', () => {
    it('reverts if the certificate is already stored', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      await expect(
        toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      ).to.be.revertedWith('Bytes32Set: key already exists in the set.')
    })

    it('reverts if non-admin tries to push certificate', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const [, nonAdmin] = await ethers.getSigners()
      await expect(
        toposCore.connect(nonAdmin).pushCertificate(defaultCert, cc.CERT_POS_1)
      ).to.be.revertedWithCustomError(toposCore, 'NotAdmin')
    })

    it('gets the certificate count', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      expect(await toposCore.getCertificateCount()).to.equal(1)
    })

    it('gets count for multiple certificates', async () => {
      const { toposCore } = await loadFixture(deployToposCoreFixture)
      const testCheckpoints = [
        [cc.CERT_ID_1, cc.CERT_POS_1, cc.SOURCE_SUBNET_ID_1],
        [cc.CERT_ID_2, cc.CERT_POS_2, cc.SOURCE_SUBNET_ID_2],
      ]

      for (const checkpoint of testCheckpoints) {
        const certificate = testUtils.encodeCertParam(
          cc.PREV_CERT_ID_0,
          checkpoint[2].toString(),
          cc.STATE_ROOT_MAX,
          cc.TX_ROOT_MAX,
          cc.RECEIPT_ROOT_MAX,
          [cc.TARGET_SUBNET_ID_4],
          cc.VERIFIER,
          checkpoint[0].toString(),
          cc.DUMMY_STARK_PROOF,
          cc.DUMMY_SIGNATURE
        )
        await toposCore.pushCertificate(certificate, checkpoint[1])
      }
      expect(await toposCore.getCertificateCount()).to.equal(2)
    })

    it('gets the certificate at a given index', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      const certificate = await toposCore.getCertIdAtIndex(0)
      expect(certificate).to.equal(cc.CERT_ID_1)
    })

    it('updates the source subnet set correctly', async () => {
      const { toposCore } = await loadFixture(deployToposCoreFixture)
      const testCheckpoints = [
        [cc.CERT_ID_1, cc.CERT_POS_1, cc.SOURCE_SUBNET_ID_1],
        [cc.CERT_ID_2, cc.CERT_POS_2, cc.SOURCE_SUBNET_ID_2],
        [cc.CERT_ID_3, cc.CERT_POS_3, cc.SOURCE_SUBNET_ID_3],
      ]

      for (const checkpoint of testCheckpoints) {
        const certificate = testUtils.encodeCertParam(
          cc.PREV_CERT_ID_0,
          checkpoint[2].toString(),
          cc.STATE_ROOT_MAX,
          cc.TX_ROOT_MAX,
          cc.RECEIPT_ROOT_MAX,
          [cc.TARGET_SUBNET_ID_4],
          cc.VERIFIER,
          checkpoint[0].toString(),
          cc.DUMMY_STARK_PROOF,
          cc.DUMMY_SIGNATURE
        )
        await toposCore.pushCertificate(certificate, checkpoint[1])
      }

      const encodedCheckpoints = await toposCore.getCheckpoints()
      const checkpoints = encodedCheckpoints.map((checkpoint) => {
        return [checkpoint[0], Number(checkpoint[1]), checkpoint[2]]
      })
      testCheckpoints.map((innerArr1, i) =>
        innerArr1.map((item, j) => expect(item).to.equal(checkpoints[i][j]))
      )
      const updatedTestCheckpoint = [
        cc.CERT_ID_4,
        cc.CERT_POS_4,
        cc.SOURCE_SUBNET_ID_2,
      ]
      const updatedCertificate = testUtils.encodeCertParam(
        cc.PREV_CERT_ID_0,
        updatedTestCheckpoint[2].toString(),
        cc.STATE_ROOT_MAX,
        cc.TX_ROOT_MAX,
        cc.RECEIPT_ROOT_MAX,
        [cc.TARGET_SUBNET_ID_4],
        cc.VERIFIER,
        updatedTestCheckpoint[0].toString(),
        cc.DUMMY_STARK_PROOF,
        cc.DUMMY_SIGNATURE
      )
      await toposCore.pushCertificate(
        updatedCertificate,
        updatedTestCheckpoint[1]
      )
      const updatedEncodedCheckpoints = await toposCore.getCheckpoints()
      const updatedCheckpoints = updatedEncodedCheckpoints.map((checkpoint) => {
        return [checkpoint[0], Number(checkpoint[1]), checkpoint[2]]
      })
      testCheckpoints[1] = updatedTestCheckpoint
      testCheckpoints.map((innerArr1, i) =>
        innerArr1.map((item, j) =>
          expect(item).to.equal(updatedCheckpoints[i][j])
        )
      )
    })

    it('gets a storage certificate', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      const certificate = await toposCore.getCertificate(cc.CERT_ID_1)
      const encodedCert = testUtils.encodeCertParam(
        certificate.prevId,
        certificate.sourceSubnetId,
        certificate.stateRoot,
        certificate.txRoot,
        certificate.receiptRoot,
        certificate.targetSubnets,
        Number(certificate.verifier),
        certificate.certId,
        certificate.starkProof,
        certificate.signature
      )
      expect(encodedCert).to.equal(defaultCert)
    })

    it('emits a certificate stored event', async () => {
      const { defaultCert, toposCore } = await loadFixture(
        deployToposCoreFixture
      )
      const tx = await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)
      await expect(tx)
        .to.emit(toposCore, 'CertStored')
        .withArgs(cc.CERT_ID_1, cc.RECEIPT_ROOT_MAX)
    })
  })

  describe('proxy', () => {
    it('reverts if the ToposCore implementation contract is not present', async () => {
      const { admin, toposCoreProxy } = await loadFixture(
        deployToposCoreFixture
      )
      await expect(
        new ToposCoreProxy__factory(admin).deploy(ethers.ZeroAddress)
      ).to.be.revertedWithCustomError(
        toposCoreProxy as ToposCoreProxy,
        'InvalidImplementation'
      )
    })
  })

  describe('upgrade', () => {
    it('reverts if the code hash does not match', async () => {
      const { toposCore, altToposCoreAddress } = await loadFixture(
        deployToposCoreFixture
      )
      const emptyCodeHash =
        '0x0000000000000000000000000000000000000000000000000000000000000000'
      await expect(
        toposCore.upgrade(altToposCoreAddress, emptyCodeHash)
      ).to.be.revertedWithCustomError(toposCore, 'InvalidCodeHash')
    })

    it('fetches the certificates from the contract after upgrade', async () => {
      const {
        admin,
        altToposCoreImplementationAddress,
        defaultCert,
        toposCore,
      } = await loadFixture(deployToposCoreFixture)
      const oldImplementationAddress = await toposCore.implementation()
      await toposCore.pushCertificate(defaultCert, cc.CERT_POS_1)

      const CodeHash = await new CodeHash__factory(admin).deploy()
      const codeHash = await CodeHash.waitForDeployment()
      const implementationCodeHash = await codeHash.getCodeHash(
        altToposCoreImplementationAddress
      )

      await toposCore.upgrade(
        altToposCoreImplementationAddress,
        implementationCodeHash
      )
      expect(await toposCore.implementation()).to.not.equal(
        oldImplementationAddress
      )
      const certificate = await toposCore.getCertificate(cc.CERT_ID_1)
      const encodedCert = testUtils.encodeCertParam(
        certificate.prevId,
        certificate.sourceSubnetId,
        certificate.stateRoot,
        certificate.txRoot,
        certificate.receiptRoot,
        certificate.targetSubnets,
        Number(certificate.verifier),
        certificate.certId,
        certificate.starkProof,
        certificate.signature
      )
      expect(encodedCert).to.equal(defaultCert)
    })

    it('emits an upgraded event', async () => {
      const { admin, altToposCoreImplementationAddress, toposCore } =
        await loadFixture(deployToposCoreFixture)
      expect(await toposCore.implementation()).to.not.equal(
        altToposCoreImplementationAddress
      )

      const CodeHash = await new CodeHash__factory(admin).deploy()
      const codeHash = await CodeHash.waitForDeployment()
      const implementationCodeHash = await codeHash.getCodeHash(
        altToposCoreImplementationAddress
      )

      await expect(
        toposCore.upgrade(
          altToposCoreImplementationAddress,
          implementationCodeHash
        )
      )
        .to.emit(toposCore, 'Upgraded')
        .withArgs(altToposCoreImplementationAddress)
      expect(await toposCore.implementation()).to.equal(
        altToposCoreImplementationAddress
      )
      const currentAdmins = await toposCore.admins(1)
      expect(currentAdmins[0]).to.equal(admin.address) // check that the admin is unchanged
    })
  })
})
