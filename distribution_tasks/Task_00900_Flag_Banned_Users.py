import json
import os

from urllib import request
from distribution_tasks.distribution_task import DistributionTask


class FlagBannedUsersDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 900

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution = super().get_current_document_version(pipeline_config['distribution'])

        temp_banned = super().get_current_document_version('temp_bans')
        perm_banned = super().get_current_document_version('perm_bans')
        all_bans = temp_banned + perm_banned
        banned_users = [b['username'] for b in all_bans]

        self.logger.info(f"  distribution size before bans: [{len(distribution)}]")

        for d in distribution:
            if d['username'] in banned_users:
                d['eligible'] = False
                d['eligiblity_reason'] = 'ban'
            else:
                d['eligible'] = True
                d['eligiblity_reason'] = ""

        self.logger.info(f"  distribution size after bans: [{len([x for x in distribution if x['eligiblity_reason'] != 'ban'])}]")

        super().save_document_version(distribution, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config)
