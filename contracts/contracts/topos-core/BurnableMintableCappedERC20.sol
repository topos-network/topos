// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

import {IBurnableMintableCappedERC20} from "./../interfaces/IBurnableMintableCappedERC20.sol";

import {MintableCappedERC20} from "./MintableCappedERC20.sol";

contract BurnableMintableCappedERC20 is IBurnableMintableCappedERC20, MintableCappedERC20 {
    constructor(
        string memory name,
        string memory symbol,
        uint256 capacity,
        uint256 initialSupply,
        address deployer,
        address operator
    ) MintableCappedERC20(name, symbol, capacity, initialSupply, deployer, operator) {}

    function burnFrom(address account, uint256 amount) external {
        require(
            hasRole(DEFAULT_ADMIN_ROLE, msg.sender) || hasRole(OPERATOR_ROLE, msg.sender),
            "BurnableMintableCappedERC20: must have the admin or operator role to burn"
        );
        uint256 _allowance = allowance(account, msg.sender);
        if (_allowance != type(uint256).max) {
            _approve(account, msg.sender, _allowance - amount);
        }
        _burn(account, amount);
    }
}
