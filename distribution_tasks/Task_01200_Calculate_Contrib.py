import os

from decimal import Decimal
from distribution_tasks.distribution_task import DistributionTask


class CalculateContribDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1200

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        # voter_data = super().get_current_document_version(pipeline_config['voter'])
        tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        base_distribution = super().get_document_version('distribution', 0)
        current_distribution = super().get_current_document_version('distribution')
        post_of_the_week_winners = super().get_current_document_version('post_of_the_week') or []

        contrib = []

        for d in current_distribution:
            tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == d['username'].lower()), None)
            mod = next((t for t in mod_rewards if t['username'].lower() == d['username'].lower()), None)
            org = next((o for o in organizer_rewards if o['username'].lower() == d['username'].lower()), None)
            base_record = next((b for b in base_distribution if b['username'].lower() == d['username'].lower()), None)
            post_of_the_week_awards = [p for p in post_of_the_week_winners if p['username'].lower() == d['username'].lower()]

            # voter = None
            # if voter_data:
            #     voter = next((v for v in voter_data if v['username'].lower() == d['username'].lower()), None)

            post_of_the_week_contrib = 0
            for award in post_of_the_week_awards:
                if award['rank'] == 1:
                    post_of_the_week_contrib += 5000
                elif award['rank'] == 2:
                    post_of_the_week_contrib += 3000
                elif award['rank'] == 3:
                    post_of_the_week_contrib += 1500
                else:
                    post_of_the_week_contrib += 500

            tip_bonus = (tip_bonus and tip_bonus['points']) or 0
            mod = (mod and mod['points']) or 0
            org = (org and org['points']) or 0
            base = (base_record and base_record['points'] or 0)
            # voter = (voter and voter['points']) or 0
            voter = Decimal(d['points_after_bonus']) - Decimal(d['points'])

            contrib_points = Decimal(tip_bonus) + Decimal(mod) + Decimal(org) + Decimal(base) + Decimal(voter) + Decimal(post_of_the_week_contrib)
            contrib_points = max(contrib_points, Decimal(0))

            contrib.append({
                'username': d['username'],
                'contrib': contrib_points,
                'base': base,
                'mod': mod,
                'org': org,
                'tip_bonus': tip_bonus,
                'voter': voter,
                'post_of_the_week': post_of_the_week_contrib
            })

        super().save_document_version(contrib, 'contrib')

        return super().update_pipeline(pipeline_config, {
            'contrib': 'contrib'
        })
