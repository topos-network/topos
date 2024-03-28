// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

contract CodeHash {
    /// @notice gets the codehash of a contract address
    /// @param contractAddr a contract address
    function getCodeHash(address contractAddr) public view returns (bytes32 codeHash) {
        // does not fail with wallet addresses
        if (contractAddr.codehash.length != 0) {
            codeHash = contractAddr.codehash;
        }
    }
}
