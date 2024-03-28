// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./../topos-core/Bytes32Sets.sol";

import "./../interfaces/IERC20Messaging.sol";
import "./../interfaces/ITokenDeployer.sol";

import {IBurnableMintableCappedERC20} from "./../interfaces/IBurnableMintableCappedERC20.sol";
import {ToposMessaging} from "./../topos-core/ToposMessaging.sol";

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

contract ERC20Messaging is IERC20Messaging, ToposMessaging {
    using Bytes32SetsLib for Bytes32SetsLib.Set;

    // Slot names should be prefixed with some standard string
    bytes32 internal constant PREFIX_TOKEN_KEY = keccak256("token-key");
    bytes32 internal constant PREFIX_TOKEN_TYPE = keccak256("token-type");
    bytes32 internal constant PREFIX_TOKEN_DAILY_MINT_LIMIT = keccak256("token-daily-mint-limit");
    bytes32 internal constant PREFIX_TOKEN_DAILY_MINT_AMOUNT = keccak256("token-daily-mint-amount");

    /// @notice Set of Token Keys derived from token symbols
    Bytes32SetsLib.Set tokenSet;

    /// @notice Internal token deployer (ERCBurnableMintable by default)
    address internal immutable _tokenDeployerAddr;

    /// @notice Mapping to store Tokens
    mapping(bytes32 => Token) public tokens;

    /// @notice Constructor for ERC20Messaging contract
    /// @param tokenDeployerAddr Address of the token deployer contract
    constructor(address tokenDeployerAddr, address toposCoreAddr) ToposMessaging(toposCoreAddr) {
        if (tokenDeployerAddr.code.length == uint256(0)) revert InvalidTokenDeployer();
        _tokenDeployerAddr = tokenDeployerAddr;
    }

    /// @notice Deploy an internal token
    /// @param params Encoded token params for deploying an internal token
    function deployToken(bytes calldata params) external {
        (string memory name, string memory symbol, uint256 cap, uint256 dailyMintLimit, uint256 initialSupply) = abi
            .decode(params, (string, string, uint256, uint256, uint256));
        // Note: this does not stop deployment of the same symbol to other subnets. Do not use in a production system.
        bytes32 salt = keccak256(abi.encodePacked(symbol));
        // Deploy the token contract
        // The tx will revert if the token already exists because the salt will be the same
        address tokenAddress = ITokenDeployer(_tokenDeployerAddr).deployToken(
            name,
            symbol,
            cap,
            initialSupply,
            msg.sender,
            address(this),
            salt
        );

        _setTokenType(symbol, TokenType.InternalBurnableFrom);
        _setTokenAddress(symbol, tokenAddress);
        _setTokenDailyMintLimit(dailyMintLimit, symbol);

        emit TokenDeployed(symbol, tokenAddress);
    }

    /// @notice Entry point for sending a cross-subnet asset transfer
    /// @dev The input data is sent to the target subnet externally
    /// @param targetSubnetId Target subnet ID
    /// @param symbol Symbol of token
    /// @param receiver Receiver's address
    /// @param amount Amount of token to send
    function sendToken(SubnetId targetSubnetId, string calldata symbol, address receiver, uint256 amount) external {
        if (_toposCoreAddr.code.length == uint256(0)) revert InvalidToposCore();
        Token memory token = getTokenBySymbol(symbol);
        _burnTokenFrom(msg.sender, symbol, amount);
        emit TokenSent(targetSubnetId, symbol, token.addr, receiver, amount);
        _emitMessageSentEvent(targetSubnetId);
    }

    /// @notice Gets the token by symbol
    /// @param symbol Symbol of token
    function getTokenBySymbol(string calldata symbol) public view returns (Token memory token) {
        bytes32 tokenKey = _getTokenKey(symbol);
        token = tokens[tokenKey];
    }

    /// @notice Get the number of tokens deployed/registered
    function getTokenCount() public view returns (uint256) {
        return tokenSet.count();
    }

    /// @notice Get the token key at the specified index
    /// @param index Index of token key
    function getTokenKeyAtIndex(uint256 index) public view returns (bytes32) {
        return tokenSet.keyAtIndex(index);
    }

    /// @notice Get the token daily mint amount
    /// @param symbol Symbol of token
    function tokenDailyMintAmount(string memory symbol) public view returns (uint256) {
        return getUint(_getTokenDailyMintAmountKey(symbol, block.timestamp / 1 days));
    }

    /// @notice Get the token daily mint limit
    /// @param symbol Symbol of token
    function tokenDailyMintLimit(string memory symbol) public view returns (uint256) {
        return getUint(_getTokenDailyMintLimitKey(symbol));
    }

    /// @notice Get the address of token deployer contract
    function tokenDeployer() public view returns (address) {
        return _tokenDeployerAddr;
    }

    /// @notice Execute a cross-subnet asset transfer
    /// @param logIndexes Array of indexes of the logs to use
    /// @param logsAddress Array of addresses of the logs
    /// @param logsData Array of data of the logs
    /// @param logsTopics Array of topics of the logs
    function _execute(
        uint256[] memory logIndexes,
        address[] memory logsAddress,
        bytes[] memory logsData,
        bytes32[][] memory logsTopics,
        SubnetId networkSubnetId
    ) internal override {
        // verify that the event was emitted by this contract on the source subnet
        uint256 tokenSentEventIndex = logIndexes[0];
        if (logsAddress[tokenSentEventIndex] != address(this)) revert InvalidOriginAddress();

        // implication on the application contract to verify the target subnet id
        // first topic is the event signature & second topic is the target subnet id
        bytes32 targetSubnetId = logsTopics[tokenSentEventIndex][1];
        if (SubnetId.unwrap(networkSubnetId) != targetSubnetId) revert InvalidSubnetId();

        (string memory symbol, , address receiver, uint256 amount) = abi.decode(
            logsData[tokenSentEventIndex],
            (string, address, address, uint256)
        );
        _mintToken(symbol, receiver, amount);
    }

    /// @notice Burn token internally
    /// @param sender Sender of token
    /// @param symbol Symbol of token
    /// @param amount Amount of token to burn
    function _burnTokenFrom(address sender, string calldata symbol, uint256 amount) internal {
        bytes32 tokenKey = _getTokenKey(symbol);
        if (!tokenSet.exists(tokenKey)) revert TokenDoesNotExist(symbol);
        if (amount == 0) revert InvalidAmount();

        TokenType tokenType = _getTokenType(symbol);
        bool burnSuccess;

        if (tokenType == TokenType.External) {
            revert UnsupportedTokenType();
        } else {
            Token memory token = tokens[tokenKey];
            burnSuccess = _callERC20Token(
                token.addr,
                abi.encodeWithSelector(IBurnableMintableCappedERC20.burnFrom.selector, sender, amount)
            );
            if (!burnSuccess) revert BurnFailed(symbol);
        }
    }

    /// @notice Low level call to external token contract
    /// @dev Sends a low-level call to the token contract
    /// @param tokenAddress Address of token contract
    /// @param callData Data to call
    function _callERC20Token(address tokenAddress, bytes memory callData) internal returns (bool) {
        // solhint-disable-next-line avoid-low-level-calls
        (bool success, bytes memory returnData) = tokenAddress.call(callData);
        return success && (returnData.length == uint256(0) || abi.decode(returnData, (bool)));
    }

    /// @notice Mint token internally
    /// @param symbol Symbol of token
    /// @param account Account to mint token to
    /// @param amount Amount of token to mint
    function _mintToken(string memory symbol, address account, uint256 amount) internal {
        bytes32 tokenKey = _getTokenKey(symbol);
        if (!tokenSet.exists(tokenKey)) revert TokenDoesNotExist(symbol);

        _setTokenDailyMintAmount(symbol, tokenDailyMintAmount(symbol) + amount);

        if (_getTokenType(symbol) == TokenType.External) {
            revert UnsupportedTokenType();
        } else {
            Token memory token = tokens[tokenKey];
            IBurnableMintableCappedERC20(token.addr).mint(account, amount);
        }
    }

    /// @notice Store the token address for the specified symbol
    /// @param symbol Symbol of token
    /// @param tokenAddress Address of token contract
    function _setTokenAddress(string memory symbol, address tokenAddress) internal {
        bytes32 tokenKey = _getTokenKey(symbol);
        tokenSet.insert(tokenKey);
        Token storage token = tokens[tokenKey];
        token.symbol = symbol;
        token.addr = tokenAddress;
    }

    /// @notice Set the token daily mint limit for a token address
    /// @param limit Daily mint limit of token
    /// @param symbol Symbol of token
    function _setTokenDailyMintLimit(uint256 limit, string memory symbol) internal {
        _setUint(_getTokenDailyMintLimitKey(symbol), limit);

        emit TokenDailyMintLimitUpdated(symbol, limit);
    }

    /// @notice Set the token daily mint amount for a token address
    /// @param symbol Symbol of token
    /// @param amount Daily mint amount of token
    function _setTokenDailyMintAmount(string memory symbol, uint256 amount) internal {
        uint256 limit = tokenDailyMintLimit(symbol);
        if (limit > 0 && amount > limit) revert ExceedDailyMintLimit(symbol);

        _setUint(_getTokenDailyMintAmountKey(symbol, block.timestamp / 1 days), amount);
    }

    /// @notice Set the token type for a token address
    /// @param symbol Symbol of token
    /// @param tokenType Type of token (external/internal)
    function _setTokenType(string memory symbol, TokenType tokenType) internal {
        _setUint(_getTokenTypeKey(symbol), uint256(tokenType));
    }

    /// @notice Get the token type for a token address
    /// @param symbol Symbol of token
    function _getTokenType(string memory symbol) internal view returns (TokenType) {
        return TokenType(getUint(_getTokenTypeKey(symbol)));
    }

    /// @notice Get the key for the token daily mint limit
    /// @param symbol Symbol of token
    function _getTokenDailyMintLimitKey(string memory symbol) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(PREFIX_TOKEN_DAILY_MINT_LIMIT, symbol));
    }

    /// @notice Get the key for the token daily mint amount
    /// @param symbol Symbol of token
    /// @param day Day of token daily mint amount
    function _getTokenDailyMintAmountKey(string memory symbol, uint256 day) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(PREFIX_TOKEN_DAILY_MINT_AMOUNT, symbol, day));
    }

    /// @notice Get the key for the token type
    /// @param symbol Symbol of token
    function _getTokenTypeKey(string memory symbol) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(PREFIX_TOKEN_TYPE, symbol));
    }

    /// @notice Get the key for the token
    /// @param symbol Symbol of token
    function _getTokenKey(string memory symbol) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(PREFIX_TOKEN_KEY, symbol));
    }
}
