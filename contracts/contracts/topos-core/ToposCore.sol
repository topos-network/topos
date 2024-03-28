// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./AdminMultisigBase.sol";
import "./Bytes32Sets.sol";

import {Initializable} from "@openzeppelin/contracts/proxy/utils/Initializable.sol";

import "./../interfaces/IToposCore.sol";

contract ToposCore is IToposCore, AdminMultisigBase, Initializable {
    using Bytes32SetsLib for Bytes32SetsLib.Set;

    /// @dev Storage slot with the address of the current implementation. `keccak256('eip1967.proxy.implementation') - 1`
    bytes32 internal constant KEY_IMPLEMENTATION =
        bytes32(0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc);

    /// @notice The subnet ID of the subnet this contract is to be deployed on
    SubnetId public networkSubnetId;

    /// @notice Nonce for cross subnet message, meant to be used in combination with `sourceSubnetId`
    /// so that the message can be uniquely identified per subnet
    uint256 private nonce;

    /// @notice Set of certificate IDs
    Bytes32SetsLib.Set certificateSet;

    /// @notice Set of source subnet IDs (subnets sending the certificates)
    Bytes32SetsLib.Set sourceSubnetIdSet;

    /// @notice Mapping to store certificates
    mapping(CertificateId => Certificate) public certificates;

    /// @notice Mapping to store the last seen certificate for a subnet
    mapping(SubnetId => IToposCore.StreamPosition) checkpoint;

    /// @notice Mapping of receipts root to the certificate ID
    mapping(bytes32 => CertificateId) public receiptRootToCertId;

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /// @notice Contract initializer
    /// @dev Can only be called once
    /// @param adminAddresses list of admins
    /// @param newAdminThreshold number of admins required to approve a call
    function initialize(address[] memory adminAddresses, uint256 newAdminThreshold) public initializer {
        // NOTE: Admin epoch is incremented to easily invalidate current admin-related state.
        uint256 newAdminEpoch = _adminEpoch() + uint256(1);
        _setAdminEpoch(newAdminEpoch);
        _setAdmins(newAdminEpoch, adminAddresses, newAdminThreshold);
    }

    /// @notice Sets the subnet ID
    /// @param _networkSubnetId The subnet ID of the subnet this contract is to be deployed on
    function setNetworkSubnetId(SubnetId _networkSubnetId) external onlyAdmin {
        networkSubnetId = _networkSubnetId;
    }

    /// @notice Upgrades the ToposCore contract implementation
    /// @dev Need to pass the setup parameters to set the admins again
    /// @param newImplementation The address of the new implementation
    /// @param newImplementationCodeHash The code hash of the new implementation
    function upgrade(address newImplementation, bytes32 newImplementationCodeHash) external override onlyAdmin {
        if (newImplementationCodeHash != newImplementation.codehash) revert InvalidCodeHash();
        _setImplementation(newImplementation);
        emit Upgraded(newImplementation);
    }

    /// @notice Push the certificate on-chain
    /// @param certBytes The certificate in byte
    /// @param position The position of the certificate
    function pushCertificate(bytes memory certBytes, uint256 position) external override onlyAdmin {
        (
            CertificateId prevId,
            SubnetId sourceSubnetId,
            bytes32 stateRoot,
            bytes32 txRoot,
            bytes32 receiptRoot,
            SubnetId[] memory targetSubnets,
            uint32 verifier,
            CertificateId certId,
            bytes memory starkProof,
            bytes memory signature
        ) = abi.decode(
                certBytes,
                (CertificateId, SubnetId, bytes32, bytes32, bytes32, SubnetId[], uint32, CertificateId, bytes, bytes)
            );

        certificateSet.insert(CertificateId.unwrap(certId)); // add certificate ID to the CRUD storage set
        Certificate storage newCert = certificates[certId];
        newCert.prevId = prevId;
        newCert.sourceSubnetId = sourceSubnetId;
        newCert.stateRoot = stateRoot;
        newCert.txRoot = txRoot;
        newCert.receiptRoot = receiptRoot;
        newCert.targetSubnets = targetSubnets;
        newCert.verifier = verifier;
        newCert.certId = certId;
        newCert.starkProof = starkProof;
        newCert.signature = signature;

        if (!sourceSubnetIdExists(sourceSubnetId)) {
            sourceSubnetIdSet.insert(SubnetId.unwrap(sourceSubnetId)); // add the source subnet ID to the CRUD storage set
        }
        IToposCore.StreamPosition storage newStreamPosition = checkpoint[sourceSubnetId];
        newStreamPosition.certId = certId;
        newStreamPosition.position = position;
        newStreamPosition.sourceSubnetId = sourceSubnetId;

        receiptRootToCertId[receiptRoot] = certId; // add certificate ID to the receipt root mapping
        emit CertStored(certId, receiptRoot);
    }

    /// @notice Emits an event to signal a cross subnet message has been sent
    /// @param targetSubnetId The subnet ID of the target subnet
    function emitCrossSubnetMessage(SubnetId targetSubnetId) external {
        nonce += 1;
        emit CrossSubnetMessageSent(targetSubnetId, networkSubnetId, nonce);
    }

    /// @notice Returns the admin epoch
    function adminEpoch() external view override returns (uint256) {
        return _adminEpoch();
    }

    /// @notice Returns the admin threshold for a given `adminEpoch`
    function adminThreshold(uint256 epoch) external view override returns (uint256) {
        return _getAdminThreshold(epoch);
    }

    /// @notice Returns the array of admins within a given `adminEpoch`.
    function admins(uint256 epoch) external view override returns (address[] memory results) {
        uint256 adminCount = _getAdminCount(epoch);
        results = new address[](adminCount);

        for (uint256 i; i < adminCount; ++i) {
            results[i] = _getAdmin(epoch, i);
        }
    }

    /// @notice Checks if a certificate exists on the ToposCore contract
    /// @param certId The Certificate ID
    function certificateExists(CertificateId certId) public view returns (bool) {
        return certificateSet.exists(CertificateId.unwrap(certId));
    }

    /// @notice Get the certificate count of the stored certificates
    function getCertificateCount() public view returns (uint256) {
        return certificateSet.count();
    }

    /// @notice Get the certificate at the specified index
    /// @param index The index of a certificate
    function getCertIdAtIndex(uint256 index) public view returns (CertificateId) {
        return CertificateId.wrap(certificateSet.keyAtIndex(index));
    }

    /// @notice Checks if an incoming subnet ID has been pushed before or not
    /// @param subnetId source subnet ID
    function sourceSubnetIdExists(SubnetId subnetId) public view returns (bool) {
        return sourceSubnetIdSet.exists(SubnetId.unwrap(subnetId));
    }

    /// @notice Get the number of stored source subnet IDs
    function getSourceSubnetIdCount() public view returns (uint256) {
        return sourceSubnetIdSet.count();
    }

    /// @notice Get the source subnet ID at the specified index
    /// @param index The index of a source subnet ID
    function getSourceSubnetIdAtIndex(uint256 index) public view returns (SubnetId) {
        return SubnetId.wrap(sourceSubnetIdSet.keyAtIndex(index));
    }

    /// @notice Get the ToposCore implmentation address
    function implementation() public view override returns (address) {
        return getAddressStorage(KEY_IMPLEMENTATION);
    }

    /// @notice Get the certificate for the provided certificate ID
    /// @param certId certificate ID
    function getCertificate(CertificateId certId) public view returns (Certificate memory storedCert) {
        storedCert = certificates[certId];
    }

    /// @notice Get the checkpoints for the received source subnet IDs
    function getCheckpoints() public view returns (IToposCore.StreamPosition[] memory checkpoints) {
        uint256 sourceSubnetIdCount = getSourceSubnetIdCount();
        checkpoints = new StreamPosition[](sourceSubnetIdCount);
        for (uint256 i; i < sourceSubnetIdCount; i++) {
            checkpoints[i] = checkpoint[getSourceSubnetIdAtIndex(i)];
        }
    }

    /// @notice Set the ToposCore implementation address
    /// @param newImplementation Address of the new implementation contract
    function _setImplementation(address newImplementation) internal {
        _setAddress(KEY_IMPLEMENTATION, newImplementation);
    }
}
