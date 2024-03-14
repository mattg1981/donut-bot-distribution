import json
import os

from web3 import Web3
from distribution_tasks.distribution_task import DistributionTask

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
# from safe import safe_tx, safe_tx_builder
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json, SafeChain


class TransactionBuilderDistributionTask(DistributionTask):

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1400

        with open(os.path.normpath("contracts/distribute_abi.json"), 'r') as f:
            self.distribute_abi = json.load(f)

        with open(os.path.normpath("contracts/arb1_contrib_abi.json"), 'r') as f:
            self.contrib_abi = json.load(f)

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_summary = super().get_current_document_version(pipeline_config['distribution_summary'])

        w3 = Web3()
        distribute_contract = w3.eth.contract(address=w3.to_checksum_address(self.config["contracts"]["arb1"]["distribute"]),
                                              abi=self.distribute_abi)

        contrib_contract = w3.eth.contract(address=w3.to_checksum_address(self.config["contracts"]["arb1"]["contrib"]),
                                               abi=self.contrib_abi)

        distribute_contract_data = distribute_contract.encodeABI("distribute", [
                    [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['points']) > 0],
                    [w3.to_wei(d['points'], 'ether') for d in distribution_summary if float(d['points']) > 0],
                    w3.to_checksum_address(self.config["contracts"]["arb1"]["donut"])
                ])

        contrib_contract_data = contrib_contract.encodeABI("mintMany", [
                [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['contrib']) > 0],
                [w3.to_wei(d['contrib'], 'ether') for d in distribution_summary if float(d['contrib']) > 0]
            ])

        transactions = [
            SafeTx(to=w3.to_checksum_address(self.config["contracts"]["arb1"]["distribute"]), value=0, data=distribute_contract_data),
            SafeTx(to=w3.to_checksum_address(self.config["contracts"]["arb1"]["contrib"]), value=0, data=contrib_contract_data),
        ]

        tx = build_tx_builder_json(SafeChain.ARB1, f"EthTrader round {super().distribution_round}", transactions)

        self.logger.info(f"  distribution round checksum: [{tx['meta']['checksum']}]")

        super().save_safe_tx(tx)

        return super().update_pipeline(pipeline_config)
