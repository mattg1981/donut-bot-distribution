import os

from datetime import datetime
from decimal import Decimal
from python_graphql_client import GraphqlClient
from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1000

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_data = super().get_current_document_version(pipeline_config['distribution'])
        tip_bonus_data = super().get_current_document_version(pipeline_config['tipping_bonus'])
        mod_rewards = super().get_current_document_version(pipeline_config['mod_rewards'])
        organizer_rewards = super().get_current_document_version(pipeline_config['organizers'])

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

        for v in voters:
            voter = next((u for u in users if u["address"].lower() == v["address"].lower()), None)

            if not voter:
                self.logger.info(f"address {voter['id']} does not exist in users.json file")
                continue

            tip_bonus = next((t for t in tip_bonus_data if t['username'].lower() == voter['username'].lower()), None)
            mod = next((t for t in mod_rewards if t['username'].lower() == voter['username'].lower()), None)
            org = next((o for o in organizer_rewards if o['username'].lower() == voter['username'].lower()), None)
            dist = next((x for x in distribution_data if x["username"].lower() == voter["username"].lower()), None)

            if not dist:
                self.logger.info(
                    f"  user {voter['username']} does not exist in distribution file, no bonus to be applied...")
                continue

            tip_bonus = (tip_bonus and tip_bonus['points']) or 0
            mod = (mod and mod['points']) or 0
            org = (org and org['points']) or 0

            # offchain tips and account funding are not in scope for voting bonuses
            old_balance = Decimal(dist['points']) + Decimal(tip_bonus) + Decimal(mod) + Decimal(org)
            awarded_amount = (old_balance * (5 + (v['qty'] - 1)) / 100)

            v['username'] = voter['username']
            v['points'] = max(awarded_amount, Decimal(0))

        voters_filename = "voter"
        super().save_document_version(voters, voters_filename)

        return super().update_pipeline(pipeline_config, {
            'voter': voters_filename
        })
