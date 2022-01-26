# -*- coding: utf-8 -*-
from datetime import datetime
from itertools import islice

from blockchain_parser.transaction import Transaction

SATOSHI_IN_BTC = 100000000


class Wasabi:
    # https://github.com/nopara73/Dumplings/blob/master/Dumplings/Constants.cs
    FIRST_BLOCK = 530500
    # https://github.com/nopara73/Dumplings/blob/master/Dumplings/Constants.cs
    FIRST_BLOCK_NO_STATIC_COORD = 610000
    # https://github.com/nopara73/Dumplings/blob/master/Dumplings/Constants.cs
    APPROX_BASE_DENOM = 0.1 * SATOSHI_IN_BTC
    # https://github.com/nopara73/Dumplings/blob/master/Dumplings/Constants.cs
    MAX_PRECISION = 0.02 * SATOSHI_IN_BTC
    #
    COORD_ADDRESSES = [
        'bc1qs604c7jv6amk4cxqlnvuxv26hv3e48cds4m0ew',
        'bc1qa24tsgchvuxsaccp8vrnkfd85hrcpafg20kmjw'
    ]


class Samourai:
    # https://github.com/nopara73/Dumplings/blob/master/Dumplings/Constants.cs
    FIRST_BLOCK = 570000
    # https://github.com/nopara73/WasabiVsSamourai/blob/master/WasabiVsSamourai/CoinJoinIndexer.cs
    MAX_POOL_FEE = 0.0011 * SATOSHI_IN_BTC
    SAMOURAI_GENESIS_MIXES = [
        'ac9566a240a5e037471b1a58ea50206062c13e1a75c0c2de3f21c7053573330a',     # 0.001
        'c6c27bef217583cca5f89de86e0cd7d8b546844f800da91d91a74039c3b40fba',     # 0.01
        '4c906f897467c7ed8690576edfcaf8b1fb516d154ef6506a2c4cab2c48821728',     # 0.01
        'a42596825352055841949a8270eda6fb37566a8780b2aec6b49d8035955d060e',     # 0.01
        '94b0da89431d8bd74f1134d8152ed1c7c4f83375e63bc79f19cf293800a83f52',     # 0.05
        'a554db794560458c102bab0af99773883df13bc66ad287c29610ad9bac138926',     # 0.05
        '792c0bfde7f6bf023ff239660fb876315826a0a52fd32e78ea732057789b2be0',     # 0.05
        'b42df707a3d876b24a22b0199e18dc39aba2eafa6dbeaaf9dd23d925bb379c59'      # 0.5
    ]
    WHIRLPOOL_SIZES = [
        0.001 * SATOSHI_IN_BTC,
        0.01 * SATOSHI_IN_BTC,
        0.05 * SATOSHI_IN_BTC,
        0.5 * SATOSHI_IN_BTC
    ]


def format_transaction(tx: Transaction, ts: datetime, block_height: int) -> dict:
    """
    Formats a transaction to only include relevant information.
    :param tx: The transaction to format
    :param ts: The timestamp of the transaction
    :param block_height: The block height of the transaction
    :return: The formatted transaction
    """
    return {
        'ts': str(ts),
        'block_height': block_height,
        'txid': tx.txid,
        'hash': tx.hash,
        'n_inputs': tx.n_inputs,
        'n_outputs': tx.n_outputs,
        'size': tx.size,
        'inputs': [{'hash': i.transaction_hash, 'index': i.transaction_index} for i in tx.inputs],
        'outputs': [{'value': o.value, 'addresses': [a.address for a in o.addresses]} for o in tx.outputs]
    }


def chunks(data: dict, size: int = 1000):
    """
    Returns slices/chunks of a dictionary.
    :param data: The dictionary to slice
    :param size: The maximum size of each chunk
    :return: Yields chunks of size :param size:
    """
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}
