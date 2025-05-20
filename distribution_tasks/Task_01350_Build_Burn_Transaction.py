import json
import os

from web3 import Web3

from distribution_tasks.distribution_task import DistributionTask
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import SafeChain, build_tx_builder_json


class BuildSummaryDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)

        with open(os.path.normpath("contracts/distribute_abi.json"), "r") as f:
            self.distribute_abi = json.load(f)

        self.priority = -1350

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(
            f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]"
        )

        # allocation = super().get_current_document_version("distribution_allocation")[0]
        # distribution_summary_total = round(
        #     float(pipeline_config["distribution_summary_total"]), 4
        # )

        w3 = Web3()
        distribute_contract = w3.eth.contract(
            address=w3.to_checksum_address(self.config["contracts"]["arb1"]["distribute"]),
            abi=self.distribute_abi,
        )

        # distribution_allocation = float(allocation["posts"]) + float(allocation["comments"])
        #
        # burn_amount = distribution_allocation - distribution_summary_total

        distribute_contract_data = distribute_contract.encode_abi(
            "distribute",
            [
                [
                    w3.to_checksum_address(
                        "0x000000000000000000000000000000000000dEaD"
                    )  # burn address
                ],
                [w3.to_wei(int(pipeline_config["burn_amount"]), "ether")],  # amount to burn
                w3.to_checksum_address(self.config["contracts"]["arb1"]["donut"]),
            ],
        )

        transaction = [
            SafeTx(
                to=w3.to_checksum_address(
                    self.config["contracts"]["arb1"]["distribute"]
                ),
                value=0,
                data=distribute_contract_data,
            ),
        ]

        tx = build_tx_builder_json(
            SafeChain.ARB1, f"EthTrader round {super().distribution_round}", transaction
        )

        super().save_safe_tx(tx, "burn")

        return super().update_pipeline(pipeline_config)
