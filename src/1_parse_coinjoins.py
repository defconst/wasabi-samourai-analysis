# -*- coding: utf-8 -*-
"""
Parses Bitcoin Core blocks for potential Wasabi/Samourai Wallet CoinJoin transactions.
"""
import os
import logging.config
import json
from datetime import datetime
from collections import defaultdict
from argparse import ArgumentParser
from operator import itemgetter

from blockchain_parser.blockchain import Blockchain
from blockchain_parser.transaction import Transaction

from utils import Wasabi, Samourai, format_transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8.8s] %(message)s",
    handlers=[logging.StreamHandler()]
)


class CoinJoinParser:
    def __init__(self) -> None:
        self.samourai_cjs = set(Samourai.SAMOURAI_GENESIS_MIXES)

    def is_coin_join(self, tx: Transaction, block_height: int) -> dict:
        """
        Checks if the transaction is either a Wasabi Wallet or Samourai Wallet CoinJoin transaction and
        returns which heuristics detected the transaction.
        :param tx: The transaction to check
        :param block_height: The block height of the transaction to check
        :return: A dict detailing whether a CoinJoin was detected, and by which heuristic
        """
        cj = {'cj_type': None, 'details': None}
        if tx.is_coinbase():
            # Coinbase transactions can't be coin joins
            cj['details'] = 'coinbase'
        else:
            if self.is_potential_samourai_coin_join(tx):
                # Potential Samourai CJ, check if inputs are known (=outputs of previous Samourai CJ)
                for i in tx.inputs:
                    if i.transaction_hash in self.samourai_cjs:
                        # TX hash of input is known, add hash of current TX to set of known Samourai CJs
                        self.samourai_cjs.add(tx.txid)
                        cj = {
                            'cj_type': 'samourai',
                            'details': 'lineage'
                        }
            else:
                # Check if any Wasabi heuristic matches
                wasabi_cj_detection = self.is_wasabi_coin_join(tx)
                if wasabi_cj_detection is not None:
                    cj = {
                        'cj_type': 'wasabi',
                        'details': wasabi_cj_detection
                    }
        if cj['cj_type'] == 'samourai' and block_height < Samourai.FIRST_BLOCK:
            cj['details'] = 'false positive'
        elif cj['cj_type'] == 'wasabi' and block_height < Wasabi.FIRST_BLOCK:
            cj['details'] = 'false positive'
        return cj

    def is_wasabi_coin_join(self, tx: Transaction) -> str:
        """
        Runs all available Wasabi Wallet CoinJoin detection heuristics and returns a pipe-separated string of
        all matching heuristics.
        :param tx: The transaction to check
        :return: A pipe-separated string of all matching heuristics or None
        """
        detection = list()
        if self.is_wasabi_coin_join_static_coord(tx):
            detection.append('coord address')
        if self.is_wasabi_coin_join_heuristic(tx):
            detection.append('heuristic')
        return '|'.join(detection) if detection else None

    @staticmethod
    def is_wasabi_coin_join_static_coord(tx: Transaction) -> bool:
        """
        Wasabi Wallet CoinJoin transactions can be detected with the following metric:
        * At least three indistinguishable output (i.e. the same output value occurs at least three times)
        * At least one output address is a Wasabi collector address (coord script)

        Example TXs:
        * https://btc.com/bf269cac2e37c7177227f4608274029e0c84bc4b7593bae01b646e93315fb66e
        :param tx: The transaction to check
        :return: True if the transaction is a Wasabi Wallet CoinJoin transaction
        """
        output_count = defaultdict(int)
        has_collector_address = False
        if tx.n_outputs >= 3:
            for output in tx.outputs:
                output_count[output.value] += 1
                for address in output.addresses:
                    if address.address.lower() in Wasabi.COORD_ADDRESSES:
                        has_collector_address = True
        return has_collector_address and any([count > 2 for count in output_count.values()])

    @staticmethod
    def is_wasabi_coin_join_heuristic(tx: Transaction) -> bool:
        """
        Wasabi Wallet post-static coordinator detection heuristic:
        * More inputs than occurrences of the most frequent output
        * The number of the most frequent output is at least 10
        * At least one output that is distinct (coordinator fee)
        * At least three distinct output values (CoinJoin, coordinator fee, at least one change)
        :param tx: The transaction to check
        :return: The if the transaction is a Wasabi Wallet CoinJoin transaction
        """
        output_count = defaultdict(int)
        if tx.n_outputs >= 10:
            for output in tx.outputs:
                output_count[output.value] += 1
            most_frequent_eq_output = max(output_count.items(), key=itemgetter(1))
            return tx.n_inputs >= most_frequent_eq_output[1] >= 10\
                and abs(Wasabi.APPROX_BASE_DENOM - most_frequent_eq_output[0]) <= Wasabi.MAX_PRECISION\
                and any([count == 1 for count in output_count.values()])\
                and len(output_count) >= 3
        return False

    @staticmethod
    def is_potential_samourai_coin_join(tx: Transaction) -> bool:
        """
        Potential Samourai Wallet CoinJoin transactions can be detected with the following metric:
        * Exactly five inputs and exactly five outputs
        * Only one distinct output value (i.e. all outputs have the same value)
        * The output value is equal to a Samourai Whirlpool size

        Example TXs:
        * https://btc.com/5fab266662be1b2705aa5790b1924978c14e7c1525d6966f5aaa129bc8f62695
        * https://www.kycp.org/#/323df21f0b0756f98336437aa3d2fb87e02b59f1946b714a7b09df04d429dec2/in

        :param tx: The transaction to check
        :return: True if the transaction is a potential Samourai Wallet CoinJoin transaction
        """
        # 5 inputs and 5 outputs
        if tx.n_inputs == 5 and tx.n_outputs == 5:
            # one distinct output value (i.e. all output values are the same)
            if len(set(output.value for output in tx.outputs)) == 1:
                # the output and input values correspond to Samourai pool sizes
                return tx.outputs[0].value in Samourai.WHIRLPOOL_SIZES
        return False


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('-b', '--bitcoin_path', default='~/.bitcoin/blocks', help='Path to Bitcoin Core blocks')
    ap.add_argument('-s', '--start', default=0, help='Block height from which to start parsing', type=int)
    ap.add_argument('-e', '--end', default=689255, help='Final block height to parse', type=int)  # 689255 == 2021-07-01 00:07:05
    ap.add_argument('-o', '--out', required=True, help='Path to store results')
    args = ap.parse_args()

    bitcoin_path = os.path.expanduser(args.bitcoin_path) if args.bitcoin_path.startswith('~') else args.bitcoin_path
    blockchain = Blockchain(bitcoin_path)
    blocks = blockchain.get_ordered_blocks(f'{bitcoin_path}/index', start=args.start, end=args.end)

    parser = CoinJoinParser()
    blkidx = 0
    start = datetime.now()
    logging.info(f'Parsing {args.end - args.start} blocks...')
    wasabi_txs = dict()
    samourai_txs = dict()
    try:
        for block in blocks:
            for transaction in block.transactions:
                result = parser.is_coin_join(transaction, block.height)
                parsed_tx = {
                    'cj': {'type': result['cj_type'], 'details': result['details']},
                    'tx': format_transaction(transaction, block.header.timestamp, block.height)
                }
                if result['cj_type'] == 'samourai':
                    logging.info(f'Found Samourai CJ, txid: {transaction.txid}')
                    samourai_txs[transaction.txid] = parsed_tx
                elif result['cj_type'] == 'wasabi':
                    logging.info(f'Found Wasabi CJ ({result["details"]}), txid: {transaction.txid}')
                    wasabi_txs[transaction.txid] = parsed_tx

            blkidx += 1
            if blkidx % 1000 == 0:
                end = datetime.now()
                logging.info(f'Parsed {blkidx}/{args.end - args.start} blocks in {(end-start).total_seconds()}s '
                             f'({round(blkidx/(args.end - args.start), 4) * 100}% done)')
    except Exception as e:
        logging.error('Error: {}'.format(e))

    with open(f'{args.out}{os.sep}found_wasabi_cjs.json', 'w') as f:
        json.dump(wasabi_txs, f)
    with open(f'{args.out}{os.sep}found_samourai_cjs.json', 'w') as f:
        json.dump(samourai_txs, f)
