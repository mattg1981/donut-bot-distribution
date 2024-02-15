import json
import os

from web3 import Web3
from distribution_tasks.distribution_task import DistributionTask

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from safe import safe_tx, safe_tx_builder
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json, SafeChain


class GnoTransactionBuilderDistributionTask(DistributionTask):
    #GNO_DISTRIBUTE_CONTRACT_ADDRESS = "0xea0B2A9a711a8Cf9F9cAddA0a8996c0EDdC44B37"
    #GNO_CONTRIB_CONTRACT_ADDRESS = "0xFc24F552fa4f7809a32Ce6EE07C09Dcd7A41988F"
    #GNO_DONUT_CONTRACT_ADDRESS = "0x524b969793a64a602342d89bc2789d43a016b13a"

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1400

        with open(os.path.normpath("contracts/distribute_abi.json"), 'r') as f:
            self.gno_distribute_abi = json.load(f)

        with open(os.path.normpath("contracts/gno_contrib_abi.json"), 'r') as f:
            self.gno_contrib_abi = json.load(f)

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_summary = super().get_current_document_version(pipeline_config['distribution_summary'])

        w3 = Web3()
        distribute_contract = w3.eth.contract(address=w3.to_checksum_address(self.config["contracts"]["gnosis"]["distribute"]),
                                              abi=self.gno_distribute_abi)

        gno_contrib_contract = w3.eth.contract(address=w3.to_checksum_address(self.config["contracts"]["gnosis"]["contrib"]),
                                               abi=self.gno_contrib_abi)

        distribute_contract_data = distribute_contract.encodeABI("distribute", [
                    [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['points']) > 0 and (d['eligible'] == 'True' or (d['eligible'] == 'False' and d['eligiblity_reason'] in ['age', 'karma']))],
                    [w3.to_wei(d['points'], 'ether') for d in distribution_summary if float(d['points']) > 0 and (d['eligible'] == 'True' or (d['eligible'] == 'False' and d['eligiblity_reason'] in ['age', 'karma']))],
                    w3.to_checksum_address(self.config["contracts"]["gnosis"]["donut"])
                ])

        contrib_contract_data = gno_contrib_contract.encodeABI("mintMany", [
                [w3.to_checksum_address(d['address']) for d in distribution_summary if float(d['contrib']) > 0],
                [w3.to_wei(d['contrib'], 'ether') for d in distribution_summary if float(d['contrib']) > 0]
            ])

        transactions = [
            SafeTx(to=w3.to_checksum_address(self.config["contracts"]["gnosis"]["distribute"]), value=0, data=distribute_contract_data),
            SafeTx(to=w3.to_checksum_address(self.config["contracts"]["gnosis"]["contrib"]), value=0, data=contrib_contract_data),
        ]

        tx = build_tx_builder_json(SafeChain.GNOSIS, f"EthTrader round {super().distribution_round}", transactions)

        self.logger.info(f"  distribution round checksum: [{tx['meta']['checksum']}]")

        super().save_safe_tx(tx)

        return super().update_pipeline(pipeline_config)
