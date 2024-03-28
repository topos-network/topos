// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

import {ITokenDeployer} from "./../interfaces/ITokenDeployer.sol";

import {BurnableMintableCappedERC20} from "./BurnableMintableCappedERC20.sol";

contract TokenDeployer is ITokenDeployer {
    function deployToken(
        string calldata name,
        string calldata symbol,
        uint256 cap,
        uint256 initialSupply,
        address deployer,
        address operator,
        bytes32 salt
    ) external returns (address tokenAddress) {
        tokenAddress = address(
            new BurnableMintableCappedERC20{salt: salt}(name, symbol, cap, initialSupply, deployer, operator)
        );
        emit Deployed(tokenAddress);
    }
}
