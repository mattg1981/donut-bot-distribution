import json
import os
import sys
from datetime import datetime
from web3 import Web3
from hashlib import sha3_256

from distribution_tasks.distribution_task import DistributionTask

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from safe import safe_tx, safe_tx_builder
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json


class GnoTransactionBuilderDistributionTask(DistributionTask):
    GNO_DISTRIBUTE_CONTRACT_ADDRESS = "0xea0B2A9a711a8Cf9F9cAddA0a8996c0EDdC44B37"
    GNO_CONTRIB_CONTRACT_ADDRESS = "0xFc24F552fa4f7809a32Ce6EE07C09Dcd7A41988F"
    GNO_DONUT_CONTRACT_ADDRESS = "0x524b969793a64a602342d89bc2789d43a016b13a"

    # GNO_MULTISIG_ADDRESS = "0x682b5664C2b9a6a93749f2159F95c23fEd654F0A"

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1300

        with open(os.path.normpath("contracts/gno_distribute_abi.json"), 'r') as f:
            self.gno_distribute_abi = json.load(f)

        with open(os.path.normpath("contracts/gno_contrib_abi.json"), 'r') as f:
            self.gno_contrib_abi = json.load(f)

    def process(self, pipeline_config):
        self.logger.info("begin task")
        super().process(pipeline_config)

        distribution_summary = super().get_current_document_version(pipeline_config['distribution_summary'])

        w3 = Web3()
        distribute_contract = w3.eth.contract(address=w3.to_checksum_address(self.GNO_DISTRIBUTE_CONTRACT_ADDRESS),
                                              abi=self.gno_distribute_abi)

        gno_contrib_contract = w3.eth.contract(address=w3.to_checksum_address(self.GNO_CONTRIB_CONTRACT_ADDRESS),
                                               abi=self.gno_contrib_abi)

        # distribute_data_sample = distribute_contract.encodeABI("distribute", [
        #     [w3.to_checksum_address("0xd762e68a2d30ab4d836683c421121AbB5b3e1DcC"),
        #      w3.to_checksum_address("0x95D9bED31423eb7d5B68511E0352Eae39a3CDD20")],
        #     [w3.to_wei(100, 'ether'), w3.to_wei(101, 'ether')],
        #     w3.to_checksum_address(self.GNO_DONUT_CONTRACT_ADDRESS)
        # ])
        #
        # contrib_data_sample = gno_contrib_contract.encodeABI("mintMany", [
        #     [w3.to_checksum_address("0xd762e68a2d30ab4d836683c421121AbB5b3e1DcC"),
        #      w3.to_checksum_address("0x95D9bED31423eb7d5B68511E0352Eae39a3CDD20")],
        #     [200, 201]
        # ])

        distribute_contract_data = distribute_contract.encodeABI("distribute", [
                    [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['points']) > 0],
                    [w3.to_wei(d['points'], 'ether') for d in distribution_summary if float(d['points']) > 0],
                    w3.to_checksum_address(self.GNO_DONUT_CONTRACT_ADDRESS)
                ])

        contrib_contract_data = gno_contrib_contract.encodeABI("mintMany", [
                [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['contrib']) > 0],
                [w3.to_wei(d['contrib'], 'ether') for d in distribution_summary if float(d['contrib']) > 0]
            ])

        transactions = [
            SafeTx(to=w3.to_checksum_address(self.GNO_DISTRIBUTE_CONTRACT_ADDRESS), value=0, data=distribute_contract_data),
            SafeTx(to=w3.to_checksum_address(self.GNO_CONTRIB_CONTRACT_ADDRESS), value=0, data=contrib_contract_data),
        ]

        # transactions = [
        #     {
        #         "to": w3.to_checksum_address(self.GNO_DISTRIBUTE_CONTRACT_ADDRESS),
        #         "value": str(0),
        #         "data": distribute_data_sample,
        #         "contractMethod": None,
        #         "contractInputsValues": None
        #     }, {
        #         "to": w3.to_checksum_address(self.GNO_CONTRIB_CONTRACT_ADDRESS),
        #         "value": str(0),
        #         "data": contrib_data_sample,
        #         "contractMethod": None,
        #         "contractInputsValues": None
        #     },
        # ]

        # tx = {
        #     "version": "1.0",
        #     "chainId": "100",
        #     "createdAt": int(datetime.now().timestamp()),
        #     "meta": {
        #         "name": "Transactions Batch",
        #         "description": f"EthTrader round {super().distribution_round} distribution",
        #         "checksum": sha3_256(json.dumps(transactions, sort_keys=True).encode('utf-8')).hexdigest(),
        #         "txBuilderVersion": "1.16.3",
        #         "createdFromSafeAddress": self.GNO_MULTISIG_ADDRESS,
        #         "createdFromOwnerAddress": ""
        #     },
        #     "transactions": transactions
        # }

        tx = build_tx_builder_json(f"EthTrader round {super().distribution_round}", transactions)

        super().save_safe_tx(tx)
        # super().save_document_version(transactions, 'tx')

        return super().update_pipeline(pipeline_config)
