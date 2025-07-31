import os

# from decimal import Decimal
from distribution_tasks.distribution_task import DistributionTask


class BuildSummaryDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1300

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(
            f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]"
        )

        distribution_data = super().get_current_document_version(
            pipeline_config["distribution"]
        )
        offchain_data = super().get_current_document_version(
            pipeline_config["offchain_data"]
        )
        contrib_data = super().get_current_document_version("contrib")
        # voter_data = super().get_current_document_version(pipeline_config['voter'])
        # tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        # mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        # organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        # base_distribution_file = super().get_document_version('distribution', 0)
        user_data = super().get_current_document_version("users")
        allocation = super().get_current_document_version("distribution_allocation")[0]
        multiplier_data = super().get_current_document_version("multiplier")

        distribution_summary = []

        # ETIP - 121
        # https://snapshot.box/#/s:ethtraderdao.eth/proposal/0x8bf37aad048618d6869b280e513d34cc148518de55b5047adfbdcd8549c22577
        # each distribution has a set amount (allocation['posts'] +  allocation['comments'] = 850k [at time of writing]) of donuts to be distributed.
        # The max points allowed to be earned per individual are set to 5% of that amount.
        max_points = (float(allocation["posts"]) + float(allocation["comments"])) * 0.05

        # ETIP - 121?
        # since donuts are capped at 5% of the total distribution, as well as other cases such as users that are banned
        # not all of the 850k donuts are distributed.  Those unallocated donuts are to be burned.
        distribution_summary_total = 0

        for d in distribution_data:
            offchain = next(
                (
                    o
                    for o in offchain_data
                    if o["user"].lower() == d["username"].lower()
                ),
                None,
            )
            # mod = next((t for t in mod_rewards if t['username'].lower() == d['username'].lower()), None)
            # org = next((o for o in organizer_rewards if o['username'].lower() == d['username'].lower()), None)
            contrib_record = next(
                (
                    c
                    for c in contrib_data
                    if c["username"].lower() == d["username"].lower()
                ),
                None,
            )
            user = next(
                (
                    u
                    for u in user_data
                    if u["username"].lower() == d["username"].lower()
                ),
                None,
            )

            multiplier = next(
                (
                    m["multiplier"]
                    for m in multiplier_data
                    if m["username"].lower() == d["username"].lower()
                ),
                None,
            )

            if not multiplier:
                self.logger.info("  >> no multiplier found for user %s", d["username"])
                multiplier = 1.0
            else:
                multiplier = float(multiplier)


            comment_score = float(d["comment_score"])
            if not int(d["eligible_comments"]):
                comment_score = 0

            post_score = float(d["post_score"])
            if not int(d["eligible_posts"]):
                post_score = 0

            points = comment_score + post_score

            # voting = max(float(d['points_after_bonus']), 0) - max(float(d['points']), 0)
            voting = float(d["voter_bonus_comments"]) + float(d["voter_bonus_posts"])
            points += voting

            # if mod:
            #     moderator = float(mod['points'])
            #     points += moderator
            # else:
            #     moderator = 0
            #
            # if org:
            #     organizer = float(org['points'])
            #     points += organizer
            # else:
            #     organizer = 0

            contrib = float((contrib_record and contrib_record["contrib"]) or 0)
            pay2post = float(d["pay2post_after_bonus"]) or 0
            points -= pay2post

            # apply the multiplier to the points.  Important to do this here so that tips and funded amounts
            # are not multiplied by the multiplier
            points *= multiplier

            if offchain:
                offchain_tips = float(offchain["tips"])
                if points <= 0:
                    points = float(offchain["points"])
                else:
                    points += float(offchain["points"])
                funded = float(offchain["funded"])
            else:
                offchain_tips = 0
                funded = 0

            if not d["eligible_comments"] and not d["eligible_posts"]:
                # -- mattg1981 -- ineligible users can now send and receive tips
                # but will not receive their base score or any bonuses as well as 0 contrib

                # return their funded amount + tips
                if offchain:
                    points = float(offchain["points"])
                else:
                    points = 0
                contrib = 0

            # remove negative values
            points = max(round(points, 4), 0)

            # cap the points at 5% of the total distribution
            points = min(points, max_points)
            distribution_summary_total += points

            distribution_summary.append(
                {
                    "username": d["username"],
                    "points": points,
                    "contrib": round(contrib, 4),
                    # 'base': round(float(base), 4),
                    # 'base': Decimal(d['points_after_bonus']),
                    # 'comment_score': d['comment_score_after_bonus'],
                    # 'post_score': d['post_score_after_bonus'],
                    "comment_score": round(
                        comment_score, 4
                    ),  # from the value in Build_Comment2Vote
                    "post_score": round(
                        post_score, 4
                    ),  # from the value in Build_Comment2Vote
                    "offchain_tips": round(offchain_tips, 4),
                    "funded": round(funded, 4),
                    "voting": round(voting, 4),
                    # 'donut_upvoter': donut_upvoter,
                    # 'quad_rank': quad_rank,
                    #'moderator': round(moderator, 4),
                    #'organizer': round(organizer, 4),
                    "pay2post": round(pay2post * -1, 4),
                    "eligible_comments": d["eligible_comments"],
                    "eligible_posts": d["eligible_posts"],
                    "eligibility_reason": d["eligibility_reason"],
                    "multiplier": multiplier,
                    "address": user["address"],
                }
            )

        # calculate and insert the total number of donuts to be burned
        distribution_allocation = float(allocation["posts"]) + float(allocation["comments"])
        burn_amount = round(distribution_allocation - distribution_summary_total, 4)
        distribution_summary.append(
            {
                "username": f"ROUND {pipeline_config["round"]} BURN",
                "points": burn_amount,
                "contrib": 0,
                "comment_score": 0,
                "post_score": 0,
                "offchain_tips": 0,
                "funded": 0,
                "voting": 0,
                "pay2post": 0,
                "eligible_comments": 0,
                "eligible_posts": 0,
                "eligibility_reason": "",
                "multiplier": 1.0,
                "address": "0x000000000000000000000000000000000000dEaD",
            }
        )

        # sort the distribution summary by points
        distribution_summary.sort(key=lambda x: float(x["points"]), reverse=True)

        distribution_summary_filename = "distribution_summary"
        super().save_document_version(
            distribution_summary, distribution_summary_filename
        )

        return super().update_pipeline(
            pipeline_config,
            {
                "distribution_summary": distribution_summary_filename,
                "distribution_summary_total": distribution_summary_total,
                "burn_amount": burn_amount,
            },
        )
