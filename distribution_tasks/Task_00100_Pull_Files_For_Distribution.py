import csv
import json
import os
import urllib

import requests

from urllib import request
from distribution_tasks.distribution_task import DistributionTask


class PullBaseFilesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 100

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        # distribution_filename = "distribution"

        # self.logger.info("  attempt to pull distribution file from the cache...")
        #
        # # test for cached file
        # base_csv = super().get_current_document_version(distribution_filename)
        #
        # if not base_csv:
        #     self.logger.info("  file not present in cache, pulling down from web...")
        #
        #     # get the csv file once it has been published
        #     url = self.config["base_csv_location"]
        #     url = url.replace("#ROUND#", str(super().distribution_round))
        #
        #     self.logger.info(f"  retrieving final csv file from base location... url [{url}]")
        #
        #     request_result = requests.get(url).text
        #     reader = csv.DictReader(request_result.splitlines(), delimiter=',')
        #     base_csv = list(reader)
        # else:
        #     self.logger.info("  cached version of distribution file detected and being used")
        #     self.logger.info("  NOTE: if there was a previous issue with this file causing a re-run to be required, "
        #                      "ensure you delete this file form the cache directory")
        #
        # base_csv_location = super().save_document_version(base_csv, distribution_filename)
        # super().cache_file(base_csv_location)

        # get distribution allocation file
        self.logger.info("  grabbing distribution allocation .json file...")
        distribution_allocation = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/allocation/distribution_allocation.json"))
        super().save_document_version(distribution_allocation, 'distribution_allocation')

        # get users file that will be used for any user <-> address lookups and gov/contrib lookups
        self.logger.info("  grabbing users.json file...")
        users_filename = "users"
        users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))
        super().save_document_version(users, users_filename)

        # get special memberships file that will be used later in the pipeline
        self.logger.info("  grabbing special membership file...")
        memberships_filename = "membership"
        membership = json.load(request.urlopen(
            "https://raw.githubusercontent.com/EthTrader/memberships/refs/heads/main/members.json"))
        super().save_document_version(membership, memberships_filename)

        # get distribution round data
        self.logger.info("  grabbing distribution round file...")
        distribution_round_filename = "distribution_round"
        distribution_round = json.load(
            request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                            f"/distribution_rounds/distribution_round_{super().distribution_round}.json"))
        super().save_document_version(distribution_round, distribution_round_filename)

        # get onchain tips - try to grab from cache first
        onchain_tips_filename = "onchain_tips"
        onchain_tips = super().get_current_document_version(onchain_tips_filename)

        if not onchain_tips:
            self.logger.info("  grabbing onchain tips file...")

            url = "https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/onchain_tips/onchain_tips.csv"
            request_result = requests.get(url).text
            reader = csv.DictReader(request_result.splitlines(), delimiter=',')
            onchain_tips = list(reader)

            onchain_tips_location = super().save_document_version(onchain_tips, onchain_tips_filename)
            super().cache_file(onchain_tips_location)
        else:
            self.logger.info("  >> grabbed onchain tips from cache")

        # get offchain tips for this round - try to grab from cache first
        self.logger.info("  grabbing offchain tips file...")
        offchain_tips_filename = "offchain_tips"
        offchain_tips = super().get_current_document_version(offchain_tips_filename)

        if not offchain_tips:
            offchain_tips = json.load(
                request.urlopen(
                    f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/offchain_tips/tips_round_{super().distribution_round}.json"))
            offchain_tips_location = super().save_document_version(offchain_tips, offchain_tips_filename)
            super().cache_file(offchain_tips_location)
        else:
            self.logger.info("  >> grabbed offchain tips from cache")

        # temp bans
        self.logger.info("  grabbing temp bans file...")
        try:
            temp_banned = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                                                f"/bans/temp_bans_round_{super().distribution_round}.json"))
        except:
            self.logger.info("  no temp bans found...")
            temp_banned = {}

        super().save_document_version(temp_banned, 'temp_bans')

        # perm bans
        self.logger.info("  grabbing perm bans file...")
        perm_banned = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                                                f"/bans/perm_bans.json"))
        super().save_document_version(perm_banned, 'perm_bans')

        # arb 1 liquidity
        self.logger.info("  grabbing liquidity file...")
        liquidity = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                                                f"/liquidity/liquidity_leaders.json"))
        super().save_document_version(liquidity, 'liquidity')

        # base raw files
        self.logger.info("  grabbing raw distribution zip file...")
        if not super().get_current_document_version("raw_zip"):
            self.logger.info("  raw zip file not present in cache, pulling down from web...")

            base_raw_url = f'https://www.mydonuts.online/home/mydonuts/static/rounds/round_{super().distribution_round}.zip'
            url_result = urllib.request.urlretrieve(base_raw_url)
            super().cache_file(super().save_document_version([{'zip_path': url_result[0]}], "raw_zip"))
        else:
            self.logger.info("  cached version of the raw zipped file have been detected and being used")
            self.logger.info("  NOTE: if there was a previous issue with this file causing a re-run to be required, "
                             "ensure you delete this file form the cache directory")

        # post of the week results
        self.logger.info("  grabbing post_of_the_week file...")
        try:
            potd_winners = json.load(request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                                                    f"/posts/potd_round_{super().distribution_round}.json"))
        except:
            self.logger.info("  no post_of_the_week file found...")
            potd_winners = []

        super().save_document_version(potd_winners, 'post_of_the_week')

        # funded accounts
        self.logger.info("  grabbing funded accounts file...")
        try:
            funded_accounts = json.load(request.urlopen(
                f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/funded_accounts/funded_round_{super().distribution_round}.json"))
        except:
            self.logger.info("  no funded accounts found...")
            funded_accounts = {}

        super().save_document_version(funded_accounts, "funded_accounts")

        return super().update_pipeline(pipeline_config, {
            'users': users_filename,
            'memberships': memberships_filename,
            'distribution_round': distribution_round_filename,
            'onchain_tips_filename': onchain_tips_filename,
            'offchain_tips': offchain_tips_filename,
            'temp_bans': 'temp_bans',
            'perm_bans': 'perm_bans',
            'funded_accounts': 'funded_accounts',
            'post_of_the_week': 'post_of_the_week',
            'liqudity': 'liquidity',
            'raw_zip': 'raw_zip'
        })
