import os

from decimal import Decimal
from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1100

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        voter_data = super().get_current_document_version(pipeline_config['voter'])
        tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        base_distribution = super().get_document_version('distribution', 0)
        current_distribution = super().get_current_document_version('distribution')

        contrib = []

        for d in current_distribution:
            tip_bonus = next((t for t in tip_bonus_data if t['username'] == d['username']), None)
            mod = next((t for t in mod_rewards if t['username'] == d['username']), None)
            org = next((o for o in organizer_rewards if o['username'] == d['username']), None)
            base_record = next((b for b in base_distribution if b['username'] == d['username']), None)
            voter = next((v for v in voter_data if v['username'] == d['username']), None)

            tip_bonus = (tip_bonus and tip_bonus['points']) or 0
            mod = (mod and mod['points']) or 0
            org = (org and org['points']) or 0
            base = (base_record and base_record['points'] or 0)
            voter = (voter and voter['points']) or 0

            contrib_points = Decimal(tip_bonus) + Decimal(mod) + Decimal(org) + Decimal(base) + Decimal(voter)
            contrib_points = max(contrib_points, Decimal(0))

            contrib.append({
                'username': d['username'],
                'contrib': contrib_points,
                'base': base,
                'mod': mod,
                'org': org,
                'tip_bonus': tip_bonus,
                'voter': voter
            })

        super().save_document_version(contrib, 'contrib')

        return super().update_pipeline(pipeline_config, {
            'contrib': 'contrib'
        })
