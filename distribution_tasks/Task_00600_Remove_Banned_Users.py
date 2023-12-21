import json
import os

from urllib import request
from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 600

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        temp_banned = super().get_current_document_version('temp_bans')
        perm_banned = super().get_current_document_version('perm_bans')

        all_bans = temp_banned + perm_banned
        banned_users = [b['username'] for b in all_bans]

        distribution = super().get_current_document_version(pipeline_config['distribution'])

        self.logger.info(f"  distribution size before bans: [{len(distribution)}]")

        distribution = [d for d in distribution if d["username"] not in banned_users]

        self.logger.info(f"  distribution size after bans: [{len(distribution)}]")

        super().save_document_version(distribution, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config)
