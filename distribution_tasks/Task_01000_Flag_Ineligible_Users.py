import os

from distribution_tasks.distribution_task import DistributionTask


class FlagIneligibleUsersDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1000

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        ineligible_users = super().get_current_document_version(pipeline_config["ineligible_users"])
        distribution = super().get_current_document_version(pipeline_config['distribution'])

        for d in distribution:
            user = next((x for x in ineligible_users if x['user'].lower() == d['username'].lower()), None)

            if user:
                d['eligible'] = False
                d['eligiblity_reason'] = user['reason']
            else:
                d['eligible'] = True
                d['eligiblity_reason'] = ""

        super().save_document_version(distribution, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config)
