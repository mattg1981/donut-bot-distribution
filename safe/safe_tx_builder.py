import json
import os
import pathlib
from datetime import datetime
from hashlib import sha3_256
from enum import Enum


class SafeChain(Enum):
    GNOSIS = 1
    ARB1 = 2


def build_tx_builder_json(chain: SafeChain, description: str, transactions: list):
    if chain == SafeChain.ARB1:
        schema_path = os.path.normpath(
            os.path.join(pathlib.Path(__file__).parent.resolve(), "tx_builder_schema_arb1.json"))
    else:
        schema_path = os.path.normpath(
            os.path.join(pathlib.Path(__file__).parent.resolve(), "tx_builder_schema_gno.json"))

    with open(schema_path, 'r') as f:
        tx = json.load(f)

    tx['meta']['description'] = description
    tx['createdAt'] = int(datetime.now().timestamp())
    tx['transactions'] = [{
        "to": t.to,
        "value": t.value,
        "data": t.data,
        "contractMethod": t.contract_method,
        "contractInputsValues": t.contract_inputs_values
    } for t in transactions]

    tx_json = json.dumps(tx['transactions'], sort_keys=True).encode('utf-8')
    tx['meta']['checksum'] = sha3_256(tx_json).hexdigest()

    return tx
