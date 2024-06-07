import os

from distribution_tasks.distribution_task import DistributionTask


class AllowSpecialMembersIfApplicableDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 300

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        eligibility_matrix = super().get_current_document_version('eligibility_matrix')
        members = super().get_current_document_version(pipeline_config['memberships'])

        special_member_names = [m['user'] for m in members]

        self.logger.info(f"  ineligible users BEFORE special memberships: [{len([
            em for em in eligibility_matrix if em['reason'] and em['reason'] not in ["special membership"]
        ])}]")

        eligible_users = [u['user'] for u in eligibility_matrix if u['user'] in special_member_names and u['reason']
                          in ['karma or age', 'comments only']]

        if eligible_users:
            self.logger.info(f"  eligible users found: {eligible_users}")

            for user in eligible_users:
                self.logger.info(f"  updating matrix for user: {user} to be able to post and comment")
                u = next(x for x in eligibility_matrix if x['user'] == user)
                u['comments'] = 1
                u['posts'] = 1

        self.logger.info(f"  ineligible users AFTER special memberships: [{len([
            em for em in eligibility_matrix if em['reason'] and em['reason'] not in ["special membership"]
        ])}]")

        super().save_document_version(eligibility_matrix, 'eligibility_matrix')

        return super().update_pipeline(pipeline_config)
