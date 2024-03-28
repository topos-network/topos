// SPDX-License-Identifier: MIT

pragma solidity ^0.8.9;

import {IERC20BurnFrom} from "./IERC20BurnFrom.sol";
import {IMintableCappedERC20} from "./IMintableCappedERC20.sol";

interface IBurnableMintableCappedERC20 is IERC20BurnFrom, IMintableCappedERC20 {
    function burnFrom(address account, uint256 amount) external;
}
