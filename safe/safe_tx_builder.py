import json
import os
import pathlib
from datetime import datetime


def build_tx_builder_json(description: str, transactions: list):
    schema_path = os.path.normpath(os.path.join(pathlib.Path(__file__).parent.resolve(), "tx_builder_schema_gno.json"))
    with open(schema_path, 'r') as f:
        tx = json.load(f)

    tx['meta']['description'] = description
    tx['createdAt'] = int(datetime.now().timestamp())
    tx['transactions'] = list().extend([{
        "to": t.to,
        "value": t.value,
        "data": t.data,
        "contractMethod": t.contract_method,
        "contractInputsValues": t.contract_inputs_values
    } for t in transactions])

    return tx
