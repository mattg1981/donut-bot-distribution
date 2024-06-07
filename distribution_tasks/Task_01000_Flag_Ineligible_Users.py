import os

from distribution_tasks.distribution_task import DistributionTask


class FlagIneligibleUsersDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1000

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        eligibility_matrix = super().get_current_document_version("eligibility_matrix")
        distribution = super().get_current_document_version(pipeline_config['distribution'])

        # add in banned users
        perm_bans = super().get_current_document_version("perm_bans")
        temp_bans = super().get_current_document_version("temp_bans")

        for pb in perm_bans:
            record = next((em for em in eligibility_matrix if pb['username'].lower() == em['user'].lower()), None)
            if record:
                record['comments'] = 0
                record['posts'] = 0
                record['reason'] = 'perma ban'
                continue

            eligibility_matrix.append({
                'user': pb['username'],
                'comments': 0,
                'posts': 0,
                'reason': 'perma ban'
            })

        for tb in temp_bans:
            record = next((em for em in eligibility_matrix if tb['username'].lower() == em['user'].lower()), None)
            if record:
                record['comments'] = 0
                record['posts'] = 0
                record['reason'] = 'temp ban'
                continue

            eligibility_matrix.append({
                'user': tb['username'],
                'comments': 0,
                'posts': 0,
                'reason': 'temp ban'
            })

        for d in distribution:
            user = next((x for x in eligibility_matrix if x['user'].lower() == d['username'].lower()), None)

            # if user is null, they were 'injected' into the distribution because they received a tip
            # we will mark them as eligible since their score should be 0
            if not user:
                d['eligibility_reason'] = ""
                d['eligible_comments'] = 1
                d['eligible_posts'] = 1
            else:
                d['eligibility_reason'] = user and user['reason'] or ""
                d['eligible_comments'] = user['comments']
                d['eligible_posts'] = user['posts']

            # if user['reason']:
            #     #d['eligible'] = False
            #     d['eligibility_reason'] = user['reason']
            #     d['eligible_comments'] = user['comments']
            #     d['eligible_posts'] = user['posts']
            # else:
            #     #d['eligible'] = True
            #     d['eligible_comments'] = user['comments']
            #     d['eligible_posts'] = user['posts']
            #     d['eligibility_reason'] = ""

        super().save_document_version(distribution, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config)
