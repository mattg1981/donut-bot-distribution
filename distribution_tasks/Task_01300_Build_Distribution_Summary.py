import os

from decimal import Decimal
from distribution_tasks.distribution_task import DistributionTask


class BuildSummaryDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1300

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_data = super().get_current_document_version(pipeline_config['distribution'])
        offchain_data = super().get_current_document_version(pipeline_config['offchain_data'])
        contrib_data = super().get_current_document_version('contrib')
        # voter_data = super().get_current_document_version(pipeline_config['voter'])
        tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        base_distribution_file = super().get_document_version('distribution', 0)
        user_data = super().get_current_document_version('users')

        distribution_summary = []

        for d in distribution_data:
            offchain = next((o for o in offchain_data if o['user'].lower() == d['username'].lower()), None)
            tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == d['username'].lower()), None)
            mod = next((t for t in mod_rewards if t['username'].lower() == d['username'].lower()), None)
            org = next((o for o in organizer_rewards if o['username'].lower() == d['username'].lower()), None)
            base_record = next((b for b in base_distribution_file if b['username'].lower() == d['username'].lower()), None)
            contrib_record = next((c for c in contrib_data if c['username'].lower() == d['username'].lower()), None)
            user = next((u for u in user_data if u['username'].lower() == d['username'].lower()), None)

            #voter = None
            #if voter_data:
                # voter = next((v for v in voter_data if v['username'].lower() == d['username'].lower()), None)

            # accounts for points + voter bonus
            points = Decimal(d['points'])
            voting = Decimal(d['points_after_bonus']) - Decimal(d['points'])

            points += voting

            # if voter:
            #     points += Decimal(voter['points'])
            #     voting = voter['points']
            # else:
            #     voting = 0

            if tip_bonus:
                points += Decimal(tip_bonus['points'])
                quad_rank = tip_bonus['quad_rank']
            else:
                quad_rank = 0

            if base_record:
                base = base_record['points']
            else:
                base = 0

            if offchain:
                if points <= 0:
                    points = Decimal(offchain['tips'])
                else:
                    points += Decimal(offchain['points'])
                offchain_tips = offchain['tips']
                funded = offchain['funded']
            else:
                offchain_tips = 0
                funded = 0

            if mod:
                points += Decimal(mod['points'])
                moderator = mod['points']
            else:
                moderator = 0

            if org:
                points += Decimal(org['points'])
                organizer = org['points']
            else:
                organizer = 0

            contrib = (contrib_record and contrib_record['contrib']) or 0
            pay2post = float(d['pay2post_after_bonus']) or 0

            # if not eligible, see if they have any funded amount that needs to
            # be returned
            # if d['eligible'].lower() != 'true':
            #     if offchain and Decimal(offchain['funded']) > 0:
            #         distribution_summary.append({
            #             'username': d['username'],
            #             'points': round(Decimal(funded), 4),
            #             'contrib': 0,
            #             'base': base,
            #             'offchain_tips': round(Decimal(offchain_tips), 4),
            #             'funded': round(Decimal(funded), 4),
            #             'voting': round(Decimal(voting), 4),
            #             'donut_upvoter': donut_upvoter,
            #             'quad_rank': quad_rank,
            #             'moderator': round(Decimal(moderator), 4),
            #             'organizer': round(Decimal(organizer), 4),
            #             'pay2post': pay2post,
            #             'address': user['address']
            #         })
            #     continue

            if d['eligible'].lower() != 'true':
                # -- mattg1981 -- ineligible users can now send and receive tips
                # but will not receive their base score or any bonuses as well as 0 contrib

                # return their funded amount + tips
                if offchain:
                    points = Decimal(offchain['points'])
                else:
                    points = 0
                contrib = 0
            else:
                # else deduct pay2post
                points -= Decimal(pay2post)

            distribution_summary.append({
                'username': d['username'],
                'points': max(round(points, 4), Decimal(0)),
                'contrib': round(float(contrib), 4),
                'base': round(float(base), 4),
                'offchain_tips': round(Decimal(offchain_tips), 4),
                'funded': round(Decimal(funded), 4),
                'voting': round(voting, 4),
                # 'donut_upvoter': donut_upvoter,
                'quad_rank': quad_rank,
                'moderator': round(Decimal(moderator), 4),
                'organizer': round(Decimal(organizer), 4),
                'pay2post': pay2post * -1,
                'eligible': d['eligible'],
                'eligibility_reason': d['eligiblity_reason'],
                'address': user['address']
            })

        distribution_summary.sort(key=lambda x: float(x['points']), reverse=True)

        distribution_summary_filename = "distribution_summary"
        super().save_document_version(distribution_summary, distribution_summary_filename)

        return super().update_pipeline(pipeline_config, {
            'distribution_summary': distribution_summary_filename
        })
