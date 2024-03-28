// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import {IToposMessaging, SubnetId} from "./IToposMessaging.sol";

interface IERC20Messaging is IToposMessaging {
    struct Token {
        string symbol;
        address addr;
    }

    enum TokenType {
        InternalBurnableFrom,
        External // Not supported yet
    }

    event TokenDailyMintLimitUpdated(string symbol, uint256 limit);

    event TokenDeployed(string symbol, address tokenAddress);

    event TokenSent(
        SubnetId indexed targetSubnetId,
        string symbol,
        address tokenAddress,
        address receiver,
        uint256 amount
    );

    error BurnFailed(string symbol);
    error ExceedDailyMintLimit(string symbol);
    error InvalidAmount();
    error InvalidOriginAddress();
    error InvalidSetDailyMintLimitsParams();
    error InvalidSubnetId();
    error InvalidTokenDeployer();
    error TokenAlreadyExists(string symbol);
    error TokenDoesNotExist(string symbol);
    error UnsupportedTokenType();

    function deployToken(bytes calldata params) external;

    function sendToken(SubnetId targetSubnetId, string calldata symbol, address receiver, uint256 amount) external;

    function getTokenBySymbol(string calldata symbol) external view returns (Token memory token);

    function getTokenCount() external view returns (uint256);

    function getTokenKeyAtIndex(uint256 index) external view returns (bytes32);

    function tokens(bytes32 tokenKey) external view returns (string memory, address);

    function tokenDailyMintAmount(string memory symbol) external view returns (uint256);

    function tokenDailyMintLimit(string memory symbol) external view returns (uint256);

    function tokenDeployer() external view returns (address);
}
