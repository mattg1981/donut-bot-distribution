import os

from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 900

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        ineligible_records = super().get_current_document_version(pipeline_config["ineligible_users"])
        ineligible_users = [i['user'] for i in ineligible_records]

        distribution = super().get_current_document_version(pipeline_config['distribution'])

        self.logger.info(f"  distribution size before ineligible users removed: [{len(distribution)}]")

        distribution = [d for d in distribution if d["username"] not in ineligible_users]

        self.logger.info(f"  distribution size after ineligible users removed: [{len(distribution)}]")

        super().save_document_version(distribution, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config)
