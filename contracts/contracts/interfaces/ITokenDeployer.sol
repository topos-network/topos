// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

interface ITokenDeployer {
    event Deployed(address indexed tokenAddress);

    function deployToken(
        string calldata name,
        string calldata symbol,
        uint256 cap,
        uint256 initialSupply,
        address deployer,
        address operator,
        bytes32 salt
    ) external returns (address tokenAddress);
}
