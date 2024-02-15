import json
import os

from decimal import Decimal
from urllib import request

import web3.constants

from distribution_tasks.distribution_task import DistributionTask

# An account was created in the community with the idea that all
# points it earns will be burned.  However, by sending to the
# zero address, that does not properly burn the token (in the
# sense that it doesnt decrement the totalSupply()).  We remove
# this account from the distribution to prevent this from happening

class RemoveBurnAccountsDistributionTask(DistributionTask):

    # ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)

        # be sure to change the priority - this value determines
        # the order the task will be executed (smaller values have higher priority)
        self.priority = 550

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        # get distribution file
        distribution = super().get_current_document_version(pipeline_config['distribution'])

        self.logger.info(f"  {len(distribution)} total accounts in distribution")

        burn_accounts = [x for x in distribution if x['blockchain_address'] == web3.constants.ADDRESS_ZERO]

        self.logger.info(f"  {len(burn_accounts)} burn accounts found..")

        for acct in burn_accounts:
            self.logger.info(f"    {acct['username']} : donuts = {acct['points']}")

        self.logger.info(f"  removing burn accounts from distribution")

        distribution = [x for x in distribution if x['blockchain_address'].lower() != web3.constants.ADDRESS_ZERO]

        self.logger.info(f"  {len(distribution)} accounts remain in distribution")

        super().save_document_version(distribution, pipeline_config["distribution"])
        super().save_document_version(burn_accounts, "burn")

        return super().update_pipeline(pipeline_config, {
            'burn': 'burn'
        })
