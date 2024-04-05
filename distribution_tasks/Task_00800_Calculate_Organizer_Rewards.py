import os

from distribution_tasks.distribution_task import DistributionTask


class ApplyOrganizerRewardsDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 800

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution = super().get_current_document_version(pipeline_config['distribution'])
        users = super().get_current_document_version('users')

        # get organizers per:
        # https://snapshot.org/#/ethtraderdao.eth/proposal/0x8ff68520b909ad93fc86643751e6cc32967d4df5f3fd43a00f50e9e80d74ed3b

        organizers = ["carlslarson", "mattg1981", "reddito321"]
        organizer_reward = 25000 / len(organizers)
        rewards = []

        for organizer in organizers:
            dist_record = next((x for x in distribution if x["username"].lower() == organizer.lower()), None)

            if not dist_record:
                address = next((u['address'] for u in users if u['username'].lower() == organizer.lower()), None)

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
                    "pay2post": '0.0',
                    "blockchain_address": address
                })

            # dist_record['points'] = float(dist_record['points']) + organizer_reward
            rewards.append({
                'username': organizer,
                'points': organizer_reward
            })

        ###
        # mattg1981 - add community managers
        ###

        community_managers = ['friendly-airline2426']
        community_manager_reward = 10000 / len(community_managers)

        for manager in community_managers:
            dist_record = next((x for x in distribution if x["username"].lower() == manager.lower()), None)

            if not dist_record:
                address = next((u['address'] for u in users if u['username'].lower() == manager.lower()), None)

                if not address:
                    self.logger.warning(
                        f"  community manager [{manager}] not found in user .csv, skipping calc...")
                    continue

                self.logger.warning(
                    f"  community manager [{manager}] does not appear in the distribution .csv, adding to file")

                distribution.append({
                    "username": manager,
                    "comments": 0,
                    "comment score": 0,
                    "post score": 0,
                    "points": 0,
                    "pay2post": '0.0',
                    "blockchain_address": address
                })

            # dist_record['points'] = float(dist_record['points']) + organizer_reward
            rewards.append({
                'username': manager,
                'points': community_manager_reward
            })

        super().save_document_version(distribution, pipeline_config['distribution'])
        super().save_document_version(rewards, "organizers")

        return super().update_pipeline(pipeline_config, {
            'organizers': "organizers"
        })
