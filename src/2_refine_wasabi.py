# -*- coding: utf-8 -*-
"""
Refines Wasabi Wallet CoinJoin detections by performing additional checks to filter out false positives.
"""
import json
import logging.config
from collections import defaultdict
from operator import itemgetter
from argparse import ArgumentParser

from utils import SATOSHI_IN_BTC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8.8s] %(message)s",
    handlers=[logging.StreamHandler()]
)


def tx_uses_gambling_address(tx: dict) -> bool:
    """
    Checks if the transaction contains known gambling address formats in its outputs.
    :param tx: The transaction to check
    :return: True if a gambling address appears in the address outputs
    """
    for output in tx['outputs']:
        for address in output['addresses']:
            return address.lower().startswith('1lucky') or address.lower().startswith('1dice')
    return False


def tx_reuses_output_address(tx: dict) -> bool:
    """
    Checks if the transaction re-uses the same output address multiple times.
    :param tx: The transaction to check
    :return: True if at least one address occurs at least twice
    """
    seen = set()
    for output in tx['outputs']:
        for address in output['addresses']:
            if address in seen:
                return True
            seen.add(address)
    return False


def tx_uses_exact_output_values(tx: dict) -> bool:
    """
    Checks if all indistinguishable outputs which an occurrence greater than 10 use values which are
    precisely 0.08, 0.09, 0.1, 0.11, or 1.12.
    :param tx: The transaction to check
    :return: True if indistinguishable outputs with a frequency of at least 10 are 0.08, 0.09, 0.1, 0.11, or 0.12
    """
    output_values = defaultdict(int)
    for output in tx['outputs']:
        output_values[output['value']] += 1
    for value, count in output_values.items():
        if count >= 10 and value in [0.08*SATOSHI_IN_BTC, 0.09*SATOSHI_IN_BTC, 0.1*SATOSHI_IN_BTC,
                                     0.11*SATOSHI_IN_BTC, 0.12*SATOSHI_IN_BTC]:
            return True
    return False


def tx_uses_disallowed_values(tx: dict) -> bool:
    """
    Checks if the transactions most frequent output value is outside of the allowed precision for Wasabi Wallets
    base denomination.
    :param tx: The transaction to check
    :return: True if the most frequent output value has a frequency of at least 10 and is outside the allowed range
    """
    output_values = defaultdict(int)
    for output in tx['outputs']:
        output_values[output['value']] += 1
    mfov = max(output_values.items(), key=itemgetter(1))
    return mfov[1] >= 10 and abs(0.1*SATOSHI_IN_BTC - mfov[0]) > 0.02*SATOSHI_IN_BTC


def tx_uses_edge_case_values(tx: dict) -> bool:
    """
    Provides an additional check by further limiting the allowed range of Wasabi Wallets most frequent output value
    to 0.0825 - 0.1175. TODO
    :param tx: The transaction to check
    :return: True if the most frequent output value has a frequency of at least 10 and is outside the allowed range
    """
    output_values = defaultdict(int)
    for output in tx['outputs']:
        output_values[output['value']] += 1
    mfov = max(output_values.items(), key=itemgetter(1))
    return mfov[1] >= 10 and abs(0.1*SATOSHI_IN_BTC - mfov[0]) > 0.015*SATOSHI_IN_BTC


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('-r', '--raw', required=True, help='Path to raw detected Wasabi Wallet CJ transactions', type=str)
    ap.add_argument('-o', '--out', required=True, help='Path to store refined Wasabi Wallet CJ transactions', type=str)
    args = ap.parse_args()

    with open(f'{args.raw}', 'r') as fp:
        txs_inputs = json.load(fp)

    metric_stats = defaultdict(int)
    filter_stats = defaultdict(int)

    results = dict()
    new_txs = dict()
    disallowed_values = set()
    filtered_ids = set()
    for tx_data in txs_inputs.values():
        tx_id = tx_data['tx']['hash']
        if tx_uses_gambling_address(tx_data['tx']):
            filter_stats['gambling'] += 1
            filtered_ids.add(tx_id)
        elif tx_uses_disallowed_values(tx_data['tx']):
            disallowed_values.add(tx_id)
            filtered_ids.add(tx_id)
        elif tx_reuses_output_address(tx_data['tx']):
            filter_stats['reuse'] += 1
            filtered_ids.add(tx_id)
        elif tx_uses_exact_output_values(tx_data['tx']):
            filter_stats['output_exact'] += 1
            filtered_ids.add(tx_id)
        elif tx_uses_edge_case_values(tx_data['tx']):
            filter_stats['output_edge'] += 1
            filtered_ids.add(tx_id)

        cj_details = tx_data['cj']['details']
        if tx_id in filtered_ids:
            if 'coord address' in cj_details:
                logging.warning(f'Coordinator address filtered: {tx_id} - '
                                f'{"(disallowed value)" if tx_id in disallowed_values else "(reason unknown)"}')
            metric_stats['fp_filtered'] += 1
        elif cj_details in ['false positive']:
            metric_stats['fp_unfiltered'] += 1
        elif cj_details == 'coord address|heuristic':
            new_txs[tx_id] = tx_data
            metric_stats['tp'] += 1
        elif cj_details == 'coord address':
            new_txs[tx_id] = tx_data
            metric_stats['fn'] += 1
        elif cj_details == 'heuristic':
            if 609999 >= tx_data['height'] >= 530500:
                metric_stats['fp_unfiltered'] += 1
            else:
                metric_stats['heuristic_positive'] += 1
                new_txs[tx_id] = tx_data

    # Basic sanity check - re-check these manually
    if sum([len(disallowed_values), filter_stats['gambling'], filter_stats['reuse'], filter_stats['output_exact'],
            filter_stats['output_edge']]) != metric_stats['fp_filtered']:
        logging.warning('UNACCOUNTED FILTER')

    logging.info('Dumping results to file...')
    with open(f'{args.out}', 'w') as fp:
        json.dump(new_txs, fp)

    # Print some details to stdout
    results = {
        '1. Total': len(txs_inputs),
        '2. Filtered': metric_stats['fp_filtered'],
        '2.1. Of which were disallowed values': len(disallowed_values),
        '2.2. Of which were known gambling addresses': filter_stats['gambling'],
        '2.3. Of which were reusing output addresses': filter_stats['reuse'],
        '2.4. Of which were using exact output values': filter_stats['output_exact'],
        '2.5. Of which were edge case values': filter_stats['output_edge'],
        '3. Final': len(txs_inputs) - len(filtered_ids),
        '4. False Positive': metric_stats['fp_unfiltered'],
        '5. True Positive': metric_stats['tp'],
        '6. False Negative': metric_stats['fn'],
        '7. Positive Heuristic': metric_stats['heuristic_positive']
    }
    print(json.dumps(results, indent=4, sort_keys=True))
