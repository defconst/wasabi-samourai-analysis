# -*- coding: utf-8 -*-
"""
Python implementation of Adam Ficsor's Wasabi Wallet CoinJoin detections.
* https://github.com/nopara73/WasabiVsSamourai/
* https://github.com/nopara73/Dumplings
"""
import os
import logging
from datetime import datetime
from argparse import ArgumentParser
import json

from blockchain_parser.blockchain import Blockchain
from blockchain_parser.transaction import Transaction

from utils import SATOSHI_IN_BTC, Samourai, format_transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8.8s] %(message)s",
    handlers=[logging.StreamHandler()]
)


def is_samourai_coin_join(tx: Transaction) -> bool:
    if tx.n_inputs == 5 and tx.n_outputs == 5:
        if len(set(output.value for output in tx.outputs)) == 1:
            for sz in Samourai.WHIRLPOOL_SIZES:
                return abs(tx.outputs[0].value - sz) <= 0.01*SATOSHI_IN_BTC
    return False


def is_samourai_coin_join_fee(tx: Transaction) -> bool:
    if tx.n_inputs == 5 and tx.n_outputs == 5:
        if len(set(output.value for output in tx.outputs)) == 1:
            for sz in Samourai.WHIRLPOOL_SIZES:
                return abs(tx.outputs[0].value - sz) <= Samourai.MAX_POOL_FEE
    return False


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('-b', '--bitcoin_path', default='~/.bitcoin/blocks', help='Path to Bitcoin Core blocks')
    ap.add_argument('-s', '--start', default=0, help='Block height from which to start parsing')
    ap.add_argument('-e', '--end', default=689255, help='Final block height to parse')
    ap.add_argument('-o', '--out', required=True, help='Path to store results')
    args = ap.parse_args()

    bitcoin_path = os.path.expanduser(args.bitcoin_path) if args.bitcoin_path.startswith('~') else args.bitcoin_path
    blockchain = Blockchain(bitcoin_path)
    blocks = blockchain.get_ordered_blocks(f'{bitcoin_path}/index', start=args.start, end=args.end)

    blkidx = 0
    start = datetime.now()
    logging.info(f'Parsing {args.end - args.start} blocks...')
    samourai_txs = dict()
    samourai_txs_fees = dict()
    try:
        for block in blocks:
            for transaction in block.transactions:
                if is_samourai_coin_join(transaction):
                    samourai_txs[transaction.txid] = format_transaction(transaction, block.header.timestamp,
                                                                        block.height)
                if is_samourai_coin_join_fee(transaction):
                    samourai_txs_fees[transaction.txid] = format_transaction(transaction, block.header.timestamp,
                                                                             block.height)

            blkidx += 1
            if blkidx % 1000 == 0:
                end = datetime.now()
                logging.info(f'Parsed {blkidx}/{args.end - args.start} blocks in {(end-start).total_seconds()}s '
                             f'({round(blkidx/(args.end - args.start), 4) * 100}% done)')
    except Exception as e:
        # TODO!
        logging.error('Error: {}'.format(e))

    with open(f'{args.out}{os.sep}nopara73_samourai_cjs_newfee.json', 'w') as f:
        json.dump(samourai_txs, f)
    with open(f'{args.out}{os.sep}nopara73_samourai_cjs_oldfee.json', 'w') as f:
        json.dump(samourai_txs_fees, f)
