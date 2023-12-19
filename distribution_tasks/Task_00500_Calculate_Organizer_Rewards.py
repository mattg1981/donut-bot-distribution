import csv
import json
import shutil
from os import path
from urllib import request

import requests

from distribution_tasks.distribution_task import DistributionTask


class ApplyOrganizerRewardsDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 500

    def process(self, pipeline_config):
        self.logger.info("begin task")
        super().process(pipeline_config)

        distribution = super().get_current_document_version(pipeline_config['distribution'])
        users = super().get_current_document_version('users')

        # get organizers
        # per https://snapshot.org/#/ethtraderdao.eth/proposal/0x8ff68520b909ad93fc86643751e6cc32967d4df5f3fd43a00f50e9e80d74ed3b

        organizers = ["carlslarson", "mattg1981", "reddito321"]
        organizer_reward = 25000 / len(organizers)
        rewards = []

        for organizer in organizers:
            dist_record = next((x for x in distribution if x["username"].lower() == organizer.lower()), None)

            if not dist_record:
                address = next((u['address'] for u in users if u['username'] == organizer), None)
                if not address:
                    self.logger.warning(
                        f"  organizer [{organizer}] not found in user .csv, skipping calc...")
                    continue

                self.logger.warning(
                    f"  organizer [{organizer}] does not appear in the distribution .csv, adding to file")

                distribution.append({
                    "username": organizer,
                    "comments": 0,
                    "comment score": 0,
                    "post score": 0,
                    "points": 0,
                    "blockchain_address": address
                })

            # dist_record['points'] = float(dist_record['points']) + organizer_reward
            rewards.append({
                'username': organizer,
                'points': organizer_reward
            })

        super().save_document_version(distribution, pipeline_config['distribution'])
        super().save_document_version(rewards, "organizers")

        return super().update_pipeline(pipeline_config, {
            'organizers': "organizers"
        })
