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

        voter_data = super().get_current_document_version('voter')
        # tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        # base_distribution = super().get_document_version('distribution', 0)
        current_distribution = super().get_current_document_version('distribution')
        users = super().get_current_document_version('users')
        post_of_the_week_winners = super().get_current_document_version('post_of_the_week') or []

        contrib = []

        for d in current_distribution:
            # tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == d['username'].lower()), None)
            user = next((u for u in users if u['username'].lower() == d['username'].lower()), None)
            mod = next((t for t in mod_rewards if t['username'].lower() == d['username'].lower()), None)
            org = next((o for o in organizer_rewards if o['username'].lower() == d['username'].lower()), None)
            # base_record = next((b for b in base_distribution if b['username'].lower() == d['username'].lower()), None)
            post_of_the_week_awards = [p for p in post_of_the_week_winners if
                                       p['username'].lower() == d['username'].lower()]

            bonus_voter_contrib = 0
            if voter_data:
                voter = next((v for v in voter_data if v['address'].lower() == d['blockchain_address'].lower()), None)
                if voter:
                    # per https://snapshot.org/#/ethtraderdao.eth/proposal/0x3740e33644bb89f91f840752212e8dd39b85e5926c48086795439fb276b94ef4
                    c = float(user['contrib'])
                    if c < 1_000_000:
                        first_vote_bonus = 500
                        additional_vote_bonus = 100
                        bonus_voter_contrib = first_vote_bonus + (additional_vote_bonus * (int(voter['qty']) - 1))

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

            # tip_bonus = (tip_bonus and tip_bonus['points']) or 0
            mod = float((mod and mod['points']) or 0)
            org = float((org and org['points']) or 0)
            # base = (base_record and base_record['points'] or 0)
            # voter = (voter and voter['points']) or 0

            comment_score = float(d['comment_score'])
            if not int(d['eligible_comments']):
                comment_score = 0

            post_score = float(d['post_score'])
            if not int(d['eligible_posts']):
                post_score = 0

            base = comment_score + post_score
            voter = max(float(d['points_after_bonus']), 0) - max(float(d['points']), 0)


            pay2post = float(d['pay2post_after_bonus'])

            contrib_points = mod + org + base + voter + post_of_the_week_contrib + bonus_voter_contrib - pay2post

            contrib_points = round(max(contrib_points, 0), 5)

            contrib.append({
                'username': d['username'],
                'contrib': contrib_points,
                'base': base,
                'mod': mod,
                'org': org,
                # 'tip_bonus': tip_bonus,
                'voter': voter,
                'voting_bonus_contrib': bonus_voter_contrib,
                'post_of_the_week': post_of_the_week_contrib,
                'pay2post': float(d['pay2post_after_bonus']) * -1,
            })

        super().save_document_version(contrib, 'contrib')

        return super().update_pipeline(pipeline_config, {
            'contrib': 'contrib'
        })
