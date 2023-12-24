import json
import os
from urllib import request

from distribution_tasks.distribution_task import DistributionTask


class ApplyModeratorBonusDistributionTask(DistributionTask):

    MOD_REWARD_POOL = 85000

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 400

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution = super().get_current_document_version(pipeline_config['distribution'])
        users = super().get_current_document_version('users')

        # get moderators
        self.logger.info("  grabbing mods file...")
        mods = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/moderators/moderators_{super().distribution_round}.json"))
        bonus_eligible_mods = [x for x in mods if int(x['bonus_eligible']) == 1 and x['community'].lower() == 'ethtrader']
        reward_amount = self.MOD_REWARD_POOL / len(bonus_eligible_mods)

        rewards = []

        for mod in bonus_eligible_mods:
            dist_record = next((x for x in distribution if x["username"].lower() == mod["name"].lower()), None)

            if not dist_record:
                address = next((u['address'] for u in users if u['username'].lower() == mod['name'].lower()), None)
                if not address:
                    self.logger.warning(
                        f"  moderator [{mod['name']}] not found in user .csv, skipping calc...")
                    continue

                self.logger.warning(
                    f"  moderator [{mod['name']}] does not appear in the distribution .csv, adding to file")

                distribution.append({
                    "username": mod['name'],
                    "comments": 0,
                    "comment score": 0,
                    "post score": 0,
                    "points": 0,
                    "pay2post": '0.0',
                    "blockchain_address": address
                })

            rewards.append({
                'username': mod['name'],
                'points': reward_amount
            })

            # dist_record['points'] = float(dist_record['points']) + reward_amount

        super().save_document_version(distribution, pipeline_config['distribution'])
        super().save_document_version(rewards, 'mod_rewards')

        return super().update_pipeline(pipeline_config, {
            'mod_rewards': 'mod_rewards'
        })
