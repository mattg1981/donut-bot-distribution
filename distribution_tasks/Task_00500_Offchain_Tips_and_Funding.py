import json
import os

from decimal import Decimal
from urllib import request
from distribution_tasks.distribution_task import DistributionTask


class DistributeOffchainTipsDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)

        # be sure to change the priority - this value determines
        # the order the task will be executed (smaller values have higher priority)
        self.priority = 500

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        # get funded accounts
        funded_accounts = super().get_current_document_version("funded_accounts")
        if not funded_accounts:
            funded_accounts = []

        # get raw offchain tips
        offchain_tips = super().get_current_document_version("offchain_tips")

        # -- start --- ineligible users are now allowed to send and receive tips

        # ineligible_users = super().get_current_document_version("ineligible_users")
        # ineligible_users = [x['user'].lower() for x in ineligible_users]

        # perm_bans = super().get_current_document_version("perm_bans")
        # perm_bans = [x['username'].lower() for x in perm_bans]

        # temp_bans = super().get_current_document_version("temp_bans")
        # temp_bans = [x['username'].lower() for x in temp_bans]

        # ineligible_users = perm_bans + temp_bans

        # -- end --

        # # get offchain tips
        # # this file is not saved in the output directory - instead, a materialized file is calculated and saved
        # self.logger.info("  offchain tips file...")
        # offchain_tips = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/offchain_tips/tips_round_{super().distribution_round}.json"))

        # list of off chain users
        offchain_users = []

        distribution = super().get_current_document_version("distribution")

        for fa in funded_accounts:
            self.logger.info(
            f"  processing funded account: [user]: {fa['user']} [amount]: {fa['amount']} [token]: {fa['token']} [tx_hash]: {fa['tx_hash']}")

            earner_record = next((x for x in distribution if x["username"].lower() == fa["user"].lower()), None)
            offchain_user = next((x for x in offchain_users if x["user"].lower() == fa["user"].lower()), None)

            if not offchain_user:
                offchain_user = {
                    'user': fa['user'],
                    'points': Decimal(fa['amount']),
                    'funded': Decimal(fa['amount']),
                    'tips': 0
                }
                offchain_users.append(offchain_user)
                continue

            # account was funded, but they did not show up in the original distribution file, we will add them there
            if not earner_record:
                self.logger.warning(f"  user [{fa['user']}] funded account but does not appear in the .csv, adding to file")
                distribution.append({
                    "username": {fa['username']},
                    "comments": 0,
                    "comment score": 0,
                    "post score": 0,
                    "points": 0,
                    "pay2post": '0.0',
                    "blockchain_address": fa['address']
                })

                continue

            old_val = offchain_user['funded']
            offchain_user['funded'] = offchain_user['points'] = old_val + Decimal(fa["amount"])
            self.logger.info(f"    previous offchain [points]: {old_val} -> new offchain [points]: {offchain_user['points']}")

        self.logger.info("completed funded accounts")

        # store the tips/amounts that actually materialize.  These will be used for tip bonus calcs
        materialized_tips = []

        self.logger.info("process tips...")

        i = 0
        for tip in offchain_tips:
            i = i + 1
            self.logger.info(
                f"tip [{i} of {len(offchain_tips)}] << [from]: {tip['from_user']} [to]:{tip['to_user']} [amount]: {tip['amount']} [token]: {tip['token']} >>")

            if not int(tip["to_user_registered"]):
                self.logger.info(f"user [{tip['to_user']}] is not registered, tip will be ignored!")
                self.logger.info("")
                continue

            # ineligible users cannot send tips but can receive them
            # if tip["from_user"].lower() in ineligible_users:
            #     self.logger.info(f"user [{tip['from_user']}] is ineligible (or banned) this round, tip will be ignored!")
            #     self.logger.info("")
            #     continue

            # if tip["to_user"].lower() in ineligible_users:
            #     self.logger.info(f"user [{tip['to_user']}] is ineligible (or banned) this round, tip will be ignored!")
            #     self.logger.info("")
            #     continue

            distribution_from = next((x for x in distribution if x["username"].lower() == tip["from_user"].lower()), None)
            distribution_to = next((x for x in distribution if x["username"].lower() == tip["to_user"].lower()), None)
            offchain_from = next((x for x in offchain_users if x["user"].lower() == tip["from_user"].lower()), None)
            offchain_to = next((x for x in offchain_users if x["user"].lower() == tip["to_user"].lower()), None)

            if not distribution_from :
                # tipper not in the distribution file
                if not distribution_from:
                    self.logger.warning(
                        f"tipper [{tip['from_user']}] not in csv, tip will not materialize... tip: {tip}")
                    continue

            if not distribution_to:
                # will need to add the receiver to the distribution file
                self.logger.warning(f"tip receiver [{tip['to_user']}] was not in distribution file, adding now...")

                distribution_to = {
                    "username": tip["to_user"],
                    "comments": 0,
                    "comment score": 0,
                    "post score": 0,
                    "points": 0,
                    "pay2post": '0.0',
                    "blockchain_address": 0
                }
                distribution.append(distribution_to)

                offchain_to = {
                    'user': tip["to_user"],
                    'points': 0,
                    'funded': 0,
                    'tips': 0
                }

                offchain_users.append(offchain_to)

            if not offchain_from:
                offchain_from = {
                    'user': tip["from_user"],
                    'points': 0,
                    'funded': 0,
                    'tips': 0
                }

                offchain_users.append(offchain_from)

            if not offchain_to:
                offchain_to = {
                    'user': tip["to_user"],
                    'points': 0,
                    'funded': 0,
                    'tips': 0
                }

                offchain_users.append(offchain_to)

            old_sender_balance = Decimal(distribution_from['points']) + offchain_from['points']
            tip_amount = Decimal(tip["amount"])

            # user didn't have enough to tip - attempt to adjust the amount or skip if the sender had <= 0 balance
            if Decimal(old_sender_balance) < tip_amount:
                self.logger.warning(
                    f"user: [{tip['from_user']}] tipped but did not have enough funds to cover the tip [prev balance: {old_sender_balance}]")

                if Decimal(distribution_from['points']) + Decimal(offchain_from['points']) <= 0:
                    self.logger.warning(f"no amount materialized, tip will be ignored...")
                    self.logger.warning("")
                    continue

                self.logger.warning(f"original tip amount: {tip_amount} -> amount materialized: {old_sender_balance}")
                tip_amount = Decimal(old_sender_balance)

            # process for tip sender
            offchain_from['points'] = round(Decimal(offchain_from['points']) - Decimal(tip_amount), 5)
            offchain_from['tips'] = round(Decimal(offchain_from['tips']) - Decimal(tip_amount), 5)

            self.logger.info(
                f"  [{tip['from_user']}] previous [points]: {old_sender_balance} -> new [points]: {Decimal(distribution_from['points']) + offchain_from['points']} (net_offchain: [{offchain_from['points']}])")

            # process for tip receiver
            old_recipient_balance = Decimal(distribution_to['points']) + offchain_to['points']
            offchain_to['points'] = round(Decimal(offchain_to['points']) + Decimal(tip_amount), 5)
            offchain_to['tips'] = round(Decimal(offchain_to['tips']) + Decimal(tip_amount), 5)

            self.logger.info(
                f"  [{tip['to_user']}] previous [points]: {old_recipient_balance} -> new [points]: {Decimal(distribution_to['points']) + offchain_to['points']} (net_offchain: [{offchain_to['points']}])")

            # add to materialized list
            materialized_tips.append({
                'from_user': tip['from_user'],
                'to_user': tip['to_user'],
                'amount': tip_amount,
                'token': tip['token'],
                'content_id': tip['content_id'],
                'parent_content_id': tip['parent_content_id'],
                'submission_content_id': tip['submission_content_id'],
                'community': tip['community'],
                'created_date': tip['created_date']
            })

            self.logger.info("")

        offchain_filename = 'offchain_data'
        materialized_tips_filename = "materialized_tips"

        super().save_document_version(offchain_users, offchain_filename)
        super().save_document_version(distribution, pipeline_config["distribution"])
        super().save_document_version(materialized_tips, materialized_tips_filename)

        return super().update_pipeline(pipeline_config, {
            "offchain_data": offchain_filename,
            "materialized_tips": materialized_tips_filename
        })
