import csv
import json
import shutil
from os import path
from urllib import request

import requests

from distribution_tasks.distribution_task import DistributionTask


class PullBaseFilesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 100

    def process(self, pipeline_config):
        self.logger.info("begin task")
        super().process(pipeline_config)

        distribution_filename = "distribution"

        # test for cached file
        base_csv = super().get_current_document_version(distribution_filename)

        if not base_csv:
            # get the csv file once it has been published
            url = self.config["base_csv_location"]
            url = url.replace("#ROUND#", str(super().distribution_round))

            self.logger.info(f"retrieving final csv file from base location... url [{url}]")

            request_result = requests.get(url).text
            reader = csv.DictReader(request_result.splitlines(), delimiter=',')
            base_csv = list(reader)
        else:
            self.logger.info("cached version of distribution file detected and being used")
            self.logger.info("NOTE: if there was a previous issue with this file causing a re-run to be required, "
                             "ensure you delete this file form the cache directory")

        base_csv_location = super().save_document_version(base_csv, distribution_filename)
        super().cache_file(base_csv_location)

        # get users file that will be used for any user <-> address lookups
        users_filename = "users"
        users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))
        super().save_document_version(users, users_filename)

        # get special memberships file that will be used later in the pipeline
        memberships_filename = "membership"
        membership = json.load(request.urlopen(
            f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main/memberships/memberships_{self.distribution_round}.json"))
        super().save_document_version(membership, memberships_filename)

        # get distribution round data
        distribution_round_filename = "distribution_round"
        distribution_round = json.load(
            request.urlopen(f"https://raw.githubusercontent.com/mattg1981/donut-bot-output/main"
                            f"/distribution_rounds/distribution_round_{super().distribution_round}.json"))
        super().save_document_version(distribution_round, distribution_round_filename)

        self.logger.info("end task")

        return super().update_pipeline(pipeline_config, {
            'distribution': distribution_filename,
            'users': users_filename,
            'memberships': memberships_filename,
            'distribution_round': distribution_round_filename
        })
