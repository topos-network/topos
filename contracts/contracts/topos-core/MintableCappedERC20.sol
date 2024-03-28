// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

import {IMintableCappedERC20} from "./../interfaces/IMintableCappedERC20.sol";

import {AccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC20Permit} from "./ERC20Permit.sol";

contract MintableCappedERC20 is IMintableCappedERC20, AccessControl, ERC20, ERC20Permit {
    bytes32 public constant OPERATOR_ROLE = keccak256("OPERATOR_ROLE");
    uint256 public immutable cap;

    /// @notice Deploy a new instance of MintableCappedERC20
    /// @dev The deployer is the initial owner of the token
    /// @param name The name of the token
    /// @param symbol The symbol of the token
    /// @param capacity The maximum amount of tokens that can be minted
    /// @param initialSupply The initial amount of tokens to mint
    /// @param deployer The address of the original deployer/token owner
    /// @param operator The address of the token operator who can mint/burn only
    constructor(
        string memory name,
        string memory symbol,
        uint256 capacity,
        uint256 initialSupply,
        address deployer,
        address operator
    ) ERC20(name, symbol) ERC20Permit(name) {
        if (capacity != 0 && initialSupply > capacity) revert CapExceeded();
        cap = capacity;
        _grantRole(DEFAULT_ADMIN_ROLE, deployer);
        _grantRole(OPERATOR_ROLE, operator);
        _mint(deployer, initialSupply);
    }

    function mint(address account, uint256 amount) external {
        require(
            hasRole(DEFAULT_ADMIN_ROLE, msg.sender) || hasRole(OPERATOR_ROLE, msg.sender),
            "MintableCappedERC20: must have the admin or operator role to mint"
        );
        uint256 capacity = cap;

        _mint(account, amount);

        if (capacity == 0) return;

        if (totalSupply() > capacity) revert CapExceeded();
    }
}
