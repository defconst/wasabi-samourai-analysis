# -*- coding: utf-8 -*-
"""
Python implementation of Adam Ficsor's Wasabi Wallet CoinJoin detections.
* https://github.com/nopara73/WasabiVsSamourai/
* https://github.com/nopara73/Dumplings
"""
import os
import logging
import json
from argparse import ArgumentParser
from datetime import datetime
from collections import defaultdict
from operator import itemgetter

from blockchain_parser.blockchain import Blockchain
from blockchain_parser.transaction import Transaction

from utils import Wasabi, format_transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8.8s] %(message)s",
    handlers=[logging.StreamHandler()]
)


def is_wasabi_coin_join_static(tx: Transaction) -> bool:
    output_count = defaultdict(int)
    has_collector_address = False
    for output in tx.outputs:
        output_count[output.value] += 1
        for address in output.addresses:
            if address.address.lower() in Wasabi.COORD_ADDRESSES:
                has_collector_address = True
    return has_collector_address and any([count > 2 for count in output_count.values()])


def is_wasabi_coin_join_heuristic(tx: Transaction) -> bool:
    output_count = defaultdict(int)
    for output in tx.outputs:
        output_count[output.value] += 1
    most_frequent_eq_output = max(output_count.items(), key=itemgetter(1))
    return tx.n_inputs >= most_frequent_eq_output[1] >= 10 \
        and abs(Wasabi.APPROX_BASE_DENOM - most_frequent_eq_output[0]) <= Wasabi.MAX_PRECISION


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('-b', '--bitcoin_path', default='~/.bitcoin/blocks', help='Path to Bitcoin Core blocks')
    ap.add_argument('-s', '--start', default=0, help='Block height from which to start parsing', type=int)
    ap.add_argument('-e', '--end', default=689255, help='Final block height to parse', type=int)
    ap.add_argument('-o', '--out', required=True, help='Path to store results')
    args = ap.parse_args()

    bitcoin_path = os.path.expanduser(args.bitcoin_path) if args.bitcoin_path.startswith('~') else args.bitcoin_path
    blockchain = Blockchain(bitcoin_path)
    blocks = blockchain.get_ordered_blocks(f'{bitcoin_path}/index', start=args.start, end=args.end)

    blkidx = 0
    start = datetime.now()
    logging.info(f'Parsing {args.end - args.start} blocks...')
    wasabi_txs_static = dict()
    wasabi_txs_heuristic = dict()
    try:
        for block in blocks:
            for transaction in block.transactions:
                if is_wasabi_coin_join_heuristic(transaction):
                    wasabi_txs_heuristic[transaction.txid] = format_transaction(transaction, block.header.timestamp,
                                                                                block.height)
                if is_wasabi_coin_join_static(transaction):
                    wasabi_txs_static[transaction.txid] = format_transaction(transaction, block.header.timestamp,
                                                                             block.height)

            blkidx += 1
            if blkidx % 1000 == 0:
                end = datetime.now()
                logging.info(f'Parsed {blkidx}/{args.end - args.start} blocks in {(end-start).total_seconds()}s '
                             f'({round(blkidx/(args.end - args.start), 4) * 100}% done)')
    except Exception as e:
        logging.error('Error: {}'.format(e))

    with open(f'{args.out}{os.sep}nopara73_wasabi_cjs_heuristic.json', 'w') as f:
        json.dump(wasabi_txs_heuristic, f)
    with open(f'{args.out}{os.sep}nopara73_wasabi_cjs_static.json', 'w') as f:
        json.dump(wasabi_txs_static, f)
