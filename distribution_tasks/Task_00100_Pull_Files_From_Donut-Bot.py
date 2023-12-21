import csv
import json
import os
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

        distribution_filename = "distribution"

        self.logger.info("  attempt to pull distribution file from the cache...")

        # test for cached file
        base_csv = super().get_current_document_version(distribution_filename)

        if not base_csv:
            self.logger.info("  file not present in cache, pulling down from web...")

            # get the csv file once it has been published
            url = self.config["base_csv_location"]
            url = url.replace("#ROUND#", str(super().distribution_round))

            self.logger.info(f"  retrieving final csv file from base location... url [{url}]")

            request_result = requests.get(url).text
            reader = csv.DictReader(request_result.splitlines(), delimiter=',')
            base_csv = list(reader)
        else:
            self.logger.info("  cached version of distribution file detected and being used")
            self.logger.info("  NOTE: if there was a previous issue with this file causing a re-run to be required, "
                             "ensure you delete this file form the cache directory")

        base_csv_location = super().save_document_version(base_csv, distribution_filename)
        super().cache_file(base_csv_location)

        # get users file that will be used for any user <-> address lookups
        self.logger.info("  grabbing users.json file...")
        users_filename = "users"
        users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))
        super().save_document_version(users, users_filename)

        # get special memberships file that will be used later in the pipeline
        self.logger.info("  grabbing special membership file...")
        memberships_filename = "membership"
        membership = json.load(request.urlopen(
            f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/memberships/memberships_{self.distribution_round}.json"))
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
            self.logger.info("  grabbed onchain tips from cache")

        # # get offchain tips for this round
        # self.logger.info("  grabbing offchain tips file...")
        # offchain_tips_filename = "offchain_tips"
        # offchain_tips = json.load(
        #     request.urlopen(
        #         f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/offchain_tips/materialized/round_{super().distribution_round}_materialized_tips.json"))
        # super().save_document_version(offchain_tips, offchain_tips_filename)

        return super().update_pipeline(pipeline_config, {
            'distribution': distribution_filename,
            'users': users_filename,
            'memberships': memberships_filename,
            'distribution_round': distribution_round_filename,
            'onchain_tips_filename': onchain_tips_filename,
            # 'offchain_tips_filename': offchain_tips_filename
        })
