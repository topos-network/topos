// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

import {IAccessControl} from "@openzeppelin/contracts/access/AccessControl.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Permit} from "./IERC20Permit.sol";

interface IMintableCappedERC20 is IERC20, IERC20Permit, IAccessControl {
    error CapExceeded();

    function mint(address account, uint256 amount) external;

    function cap() external view returns (uint256);
}
