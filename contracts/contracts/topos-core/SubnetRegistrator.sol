// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./Bytes32Sets.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import {Initializable} from "@openzeppelin/contracts/proxy/utils/Initializable.sol";

type SubnetId is bytes32;

contract SubnetRegistrator is Initializable, Ownable {
    using Bytes32SetsLib for Bytes32SetsLib.Set;

    struct Subnet {
        uint256 chainId;
        string currencySymbol;
        string endpointHttp;
        string endpointWs;
        string logoURL;
        string name;
    }

    /// @notice Set of subnet IDs
    Bytes32SetsLib.Set subnetSet;

    /// @notice Mapping to store the registered subnets
    /// @dev SubnetId => Subnet
    mapping(SubnetId => Subnet) public subnets;

    /// @notice New subnet registration event
    event NewSubnetRegistered(SubnetId subnetId);

    /// @notice Subnet removal event
    event SubnetRemoved(SubnetId subnetId);

    /// @notice Contract initializer
    /// @dev Can only be called once
    /// @param admin address of the admin
    function initialize(address admin) public initializer {
        _transferOwnership(admin);
    }

    /// @notice Check if the subnet is already registered
    /// @param subnetId FROST public key of a subnet
    function subnetExists(SubnetId subnetId) external view returns (bool) {
        return subnetSet.exists(SubnetId.unwrap(subnetId));
    }

    /// @notice Gets the count of the registered subnets
    function getSubnetCount() external view returns (uint256) {
        return subnetSet.count();
    }

    /// @notice Gets the subnet Id at the provided Index
    /// @param index index at which the Subnet ID is stored
    function getSubnetIdAtIndex(uint256 index) external view returns (SubnetId) {
        return SubnetId.wrap(subnetSet.keyAtIndex(index));
    }

    /// @notice Register a new subnet
    /// @param chainId subnet network ID
    /// @param currencySymbol currencySymbol for a subnet currency
    /// @param endpointHttp JSON RPC http endpoint of a subnet
    /// @param endpointWs JSON RPC ws endpoint of a subnet
    /// @param logoURL URL for the logo of a subnet
    /// @param name name of a subnet
    /// @param subnetId FROST public key of a subnet
    function registerSubnet(
        uint256 chainId,
        string calldata currencySymbol,
        string calldata endpointHttp,
        string calldata endpointWs,
        string calldata logoURL,
        string calldata name,
        SubnetId subnetId
    ) public onlyOwner {
        subnetSet.insert(SubnetId.unwrap(subnetId));
        Subnet storage subnet = subnets[subnetId];
        subnet.chainId = chainId;
        subnet.currencySymbol = currencySymbol;
        subnet.endpointHttp = endpointHttp;
        subnet.endpointWs = endpointWs;
        subnet.logoURL = logoURL;
        subnet.name = name;
        emit NewSubnetRegistered(subnetId);
    }

    /// @notice Remove an already registered subnet
    /// @param subnetId FROST public key of a subnet
    function removeSubnet(SubnetId subnetId) public onlyOwner {
        subnetSet.remove(SubnetId.unwrap(subnetId));
        delete subnets[subnetId];
        emit SubnetRemoved(subnetId);
    }
}
