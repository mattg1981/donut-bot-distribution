import os

from datetime import datetime
from decimal import Decimal
from python_graphql_client import GraphqlClient
from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1100

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_data = super().get_current_document_version(pipeline_config['distribution'])
        # tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        # mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        # organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])
        distribution_allocation = super().get_current_document_version("distribution_allocation")[0]

        distribution_round = super().get_current_document_version(pipeline_config['distribution_round'])
        users = super().get_current_document_version(pipeline_config['users'])

        dr_start_date = datetime.strptime(distribution_round[0]['from_date'], '%Y-%m-%d %H:%M:%S')
        dr_end_date = datetime.strptime(distribution_round[0]['to_date'], '%Y-%m-%d %H:%M:%S.%f')

        client = GraphqlClient(endpoint="https://hub.snapshot.org/graphql")

        proposals = """
                query {
                    proposals (
                        where: {
                          space_in: ["ethtraderdao.eth"],
                          state: "closed"
                        },
                        orderBy: "created",
                        orderDirection: desc
                      ) {
                        id      
                        end
                        title
                    }}
            """

        proposals_result = client.execute(query=proposals)

        voters = []

        for p in proposals_result['data']['proposals']:
            proposal_end = datetime.fromtimestamp(p['end'])

            # if it is an ignored proposal, continue
            if p["id"] in [x["id"] for x in self.config['snapshot.org']["ignore"]]:
                continue

            # if proposal not in this round, continue on to the next one
            if not (dr_start_date <= proposal_end <= dr_end_date):
                continue

            self.logger.info(f"proposal found for this round... [title] {p['title']}")

            voters_query = """
                    query Votes {
                      votes (
                        first: 1000
                        where: {
                          proposal: "#ID"
                        }
                      ) {
                        id
                        voter
                      }
                    }"""

            voters_query = voters_query.replace("#ID", p['id'])
            voters_result = client.execute(query=voters_query)

            for v in voters_result['data']['votes']:
                voter = next((x for x in voters if v["voter"].lower() == x["address"].lower()), None)

                if not voter:
                    voters.append({
                        'address': v['voter'],
                        'qty': 1
                    })
                else:
                    voter['qty'] = voter['qty'] + 1

        # fix for snapshot.org subgraph issue not allowing users to vote
        if self.distribution_round == 137:
            voters.extend([
                {
                    'address': next(u['address'] for u in users if u["username"] == 'peppers_'),
                    'qty': 12
                },
                {
                    'address': next(u['address'] for u in users if u["username"] == 'F-machine'),
                    'qty': 12
                },
                {
                    'address': next(u['address'] for u in users if u["username"] == 'CymandeTV'),
                    'qty': 12
                },
                {
                    'address': next(u['address'] for u in users if u["username"] == 'Lillica_Golden_SHIB'),
                    'qty': 12
                },
                {
                    'address': next(u['address'] for u in users if u["username"] == 'ChemicalAnybody6229'),
                    'qty': 12
                }
            ])

        # append additional information for debugging
        for v in voters:
            v['username'] = next(u['username'] for u in users if u['address'] == v['address'])

        super().save_document_version(voters, 'voter')

        for dist in distribution_data:
            voter = next((v for v in voters if dist["blockchain_address"].lower() == v["address"].lower()), None)
            # user = next((u for u in users if dist["username"].lower() == u["username"].lower()), None)
            # tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == user['username'].lower()), None)
            # mod = next((t for t in mod_rewards if t['username'].lower() == user['username'].lower()), None)
            # org = next((o for o in organizer_rewards if o['username'].lower() == user['username'].lower()), None)

            # tip_bonus = Decimal((tip_bonus and tip_bonus['points']) or 0)
            # mod = Decimal((mod and mod['points']) or 0)
            # org = Decimal((org and org['points']) or 0)

            # # remove ineligible users
            # if not dist['eligible'] == 'True':
            #     dist['comment_upvotes'] = 0
            #     dist['comment_score'] = 0
            #     dist['post_score'] = 0
            #     dist['points'] = 0
            #     dist['post_upvotes'] = 0
            #     dist['voter_bonus_comments'] = 0
            #     dist['comment_upvotes_with_bonus'] = Decimal((dist and dist['comment_upvotes']) or 0)
            #     dist['voter_bonus_posts'] = 0
            #     dist['post_upvotes_with_bonus'] = Decimal((dist and dist['post_upvotes']) or 0)
            #     dist['mod_bonus'] = mod
            #     dist['org_bonus'] = org
            #     dist['tip_bonus'] = tip_bonus
            #     continue

            if not voter:
                dist['voter_bonus_comments'] = 0
                dist['comment_upvotes_with_bonus'] = Decimal((dist and dist['comment_upvotes']) or 0)
                dist['voter_bonus_posts'] = 0
                dist['post_upvotes_with_bonus'] = Decimal((dist and dist['post_upvotes']) or 0)
                # dist['mod_bonus'] = mod
                # dist['org_bonus'] = org
                # dist['tip_bonus'] = tip_bonus
                continue


            # calculate the multiplier for this user based on their voting activities for this round
            # 10% for first and 2.5% per additional per
            # https://snapshot.org/#/ethtraderdao.eth/proposal/0x6571f3693df78a9c45f4797fb2e264e5120626b19fd80fa0b4348fabde0dadef
            first_vote_multiplier = 10
            additional_vote_multiplier = 2.5
            bonus_multiplier = round(Decimal((first_vote_multiplier + ((voter['qty'] - 1) * additional_vote_multiplier)) / 100), 4)

            comment_upvotes = Decimal((dist and dist['comment_upvotes']) or 0)
            post_upvotes = Decimal((dist and dist['post_upvotes']) or 0)

            voter_bonus_comments = round(comment_upvotes * bonus_multiplier, 5)
            voter_bonus_posts = round(post_upvotes * bonus_multiplier, 5)

            dist['voter_bonus_comments'] = voter_bonus_comments
            dist['comment_upvotes_with_bonus'] = comment_upvotes + voter_bonus_comments
            dist['voter_bonus_posts'] = voter_bonus_posts
            dist['post_upvotes_with_bonus'] = post_upvotes + voter_bonus_posts
            # dist['mod_bonus'] = mod * bonus_multiplier
            # dist['org_bonus'] = org * bonus_multiplier
            # dist['tip_bonus'] = tip_bonus * bonus_multiplier

        # calculate new comment and post ratio

        #original_comment_score = sum(float(dist['comment_upvotes'] or 0) for dist in distribution_data)
        total_comment_score_after_bonus = sum(dist['comment_upvotes_with_bonus'] for dist in distribution_data)

        #old_comment_ratio = int(distribution_allocation['comments']) / original_comment_score
        new_comment_ratio = round(Decimal(int(distribution_allocation['comments']) / total_comment_score_after_bonus), 5)

        #original_post_score = sum(float(dist['post_upvotes'] or 0) for dist in distribution_data)
        total_post_score_after_bonus = round(sum(dist['post_upvotes_with_bonus'] for dist in distribution_data), 5)

        #old_post_ratio = int(distribution_allocation['posts']) / original_post_score
        new_post_ratio = round(Decimal(int(distribution_allocation['posts']) / total_post_score_after_bonus), 5)

        #old_pay2post_ratio = float(int(distribution_allocation['posts']) / original_post_score)
        new_pay2post_ratio = float(int(distribution_allocation['posts']) / total_post_score_after_bonus)

        #self.logger.info(f"original comment ratio was: {old_comment_ratio}")
        self.logger.info(f"original comment ratio was: {pipeline_config['comment_ratio']}")
        self.logger.info(f"comment ratio (after voting bonus) is now: {new_comment_ratio}")
        #self.logger.info(f"original post ratio was: {old_post_ratio}")
        self.logger.info(f"original post ratio was: {pipeline_config['post_ratio']}")
        self.logger.info(f"post ratio (after voting bonus) is now: {new_post_ratio}")
        # self.logger.info(f"original pay2post: {old_pay2post_ratio}")
        self.logger.info(f"original pay2post: {pipeline_config['p2p_ratio']}")
        self.logger.info(f"pay2post ratio (after voting bonus) is now: {new_pay2post_ratio}")

        # apply the new ratio to each user

        for dist in distribution_data:
            comment_score_after_bonus = dist['comment_upvotes_with_bonus'] * new_comment_ratio
            post_score_after_bonus = dist['post_upvotes_with_bonus'] * new_post_ratio
            # mod_bonus = dist['mod_bonus']
            # org_bonus = dist['org_bonus']
            # tip_bonus = dist['tip_bonus']

            p2p_ratio = min(new_pay2post_ratio * 2.5, 250)
            p2p_penalty = round(p2p_ratio * float(dist['total_posts']), 5)

            dist['comment_score_after_bonus'] = comment_score_after_bonus
            dist['post_score_after_bonus'] = post_score_after_bonus
            dist['pay2post_after_bonus'] = p2p_penalty
            dist['points_after_bonus'] = round(comment_score_after_bonus +
                                               post_score_after_bonus - Decimal(p2p_penalty), 5)
                                               # mod_bonus +
                                               # org_bonus +
                                               # tip_bonus, 5)

        # for v in voters:
        #     voter = next((u for u in users if u["address"].lower() == v["address"].lower()), None)
        #
        #     if not voter:
        #         self.logger.info(f"address {voter['id']} does not exist in users.json file")
        #         continue
        #
        #     tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == voter['username'].lower()), None)
        #     mod = next((t for t in mod_rewards if t['username'].lower() == voter['username'].lower()), None)
        #     org = next((o for o in organizer_rewards if o['username'].lower() == voter['username'].lower()), None)
        #     dist = next((x for x in distribution_data if x["username"].lower() == voter["username"].lower()), None)
        #
        #     if not dist:
        #         self.logger.info(
        #             f"  user {voter['username']} does not exist in distribution file, no bonus to be applied...")
        #         continue
        #
        #     tip_bonus = (tip_bonus and tip_bonus['points']) or 0
        #     mod = (mod and mod['points']) or 0
        #     org = (org and org['points']) or 0
        #
        #     # offchain tips and account funding are not in scope for voting bonuses
        #     old_balance = Decimal(dist['points']) + Decimal(tip_bonus) + Decimal(mod) + Decimal(org)
        #     awarded_amount = (old_balance * (5 + (v['qty'] - 1)) / 100)
        #
        #     v['username'] = voter['username']
        #     v['points'] = max(awarded_amount, Decimal(0))
        #
        # voters_filename = "voter"
        # super().save_document_version(voters, voters_filename)

        # return super().update_pipeline(pipeline_config, {
        #     'voter': voters_filename
        # })

        super().save_document_version(distribution_data, pipeline_config['distribution'])

        return super().update_pipeline(pipeline_config,  {
            "new_comment_ratio": new_comment_ratio,
            "new_post_ratio": new_post_ratio,
            "new_p2p_ratio": new_pay2post_ratio,
        })
