import { RLP } from '@ethereumjs/rlp'
import { Trie } from '@ethereumjs/trie'
import { HardhatEthersProvider } from '@nomicfoundation/hardhat-ethers/internal/hardhat-ethers-provider'
import { Block, hexlify, TransactionResponse } from 'ethers'

export async function getReceiptMptProof(
  tx: TransactionResponse,
  provider: HardhatEthersProvider
) {
  const prefetchTxs = true
  const block = await provider.getBlock(tx.blockHash!, prefetchTxs)
  const rawBlock = await provider.send('eth_getBlockByHash', [
    tx.blockHash,
    prefetchTxs,
  ])

  const receiptsRoot = rawBlock.receiptsRoot
  const trie = await createTrie(block!)
  const trieRoot = trie.root()
  if ('0x' + trieRoot.toString('hex') !== receiptsRoot) {
    throw new Error(
      'Receipts root does not match trie root' +
        '\n' +
        'trieRoot: ' +
        '0x' +
        trieRoot.toString('hex') +
        '\n' +
        'receiptsRoot: ' +
        receiptsRoot
    )
  }

  const indexOfTx = block!.prefetchedTransactions.findIndex(
    (_tx) => _tx.hash === tx.hash
  )
  const key = Buffer.from(RLP.encode(indexOfTx))

  const { stack: _stack } = await trie.findPath(key)
  const stack = _stack.map((node) => node.raw())
  const proofBlob = hexlify(RLP.encode([1, indexOfTx, stack]))
  return { proofBlob, receiptsRoot }
}

async function createTrie(block: Block) {
  const trie = new Trie()
  await Promise.all(
    block.prefetchedTransactions.map(async (tx, index) => {
      const receipt = await tx.wait()
      const { cumulativeGasUsed, logs, logsBloom, status } = receipt!

      return trie.put(
        Buffer.from(RLP.encode(index)),
        Buffer.from(
          RLP.encode([
            status,
            Number(cumulativeGasUsed),
            logsBloom,
            logs.map((l) => [l.address, l.topics as string[], l.data]),
          ])
        )
      )
    })
  )
  return trie
}
