import os

from distribution_tasks.distribution_task import DistributionTask


class UpdatedUsersWeightDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)

        # be sure to change the priority - this value determines
        # the order the task will be executed (smaller values have higher priority)
        self.priority = 250

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        user_weights = super().get_current_document_version("user_weights")
        users = super().get_current_document_version('users')

        for u in users:
            weight = next((w for w in user_weights if w['tipper'].lower() == u['username'].lower()), None)

            if not weight:
                continue

            self.logger.info(f"  updating weight for: [{u['username']}]")

            u['donut'] = weight['donut']
            u['contrib'] = weight['contrib']
            u['weight'] = weight['weight']

        super().save_document_version(users, "users")

        return super().update_pipeline(pipeline_config)
