import csv
import json
import os
import shutil
from datetime import datetime, timedelta
from decimal import Decimal
from os import path
from time import sleep
from urllib import request

import praw
import prawcore
import requests
from dotenv import load_dotenv
from python_graphql_client import GraphqlClient

from distribution_tasks.distribution_task import DistributionTask


class ApplyVotingIncentivesDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 800

    def process(self, pipeline_config):
        self.logger.info("begin task")
        super().process(pipeline_config)

        ineligible_users = super().get_current_document_version(pipeline_config['ineligible_users'])
        members = super().get_current_document_version(pipeline_config['memberships'])

        special_member_names = [m['user'] for m in members]

        self.logger.info(f"ineligible users size before special memberships: [{len(ineligible_users)}]")

        eligible_users = [u['user'] for u in ineligible_users if u['user'] in special_member_names and u['reason'] in ['age', 'karma']]

        if eligible_users:
            self.logger.info(f"eligible users found: {eligible_users}")

        ineligible_users = [u for u in ineligible_users if u['user'] not in eligible_users]

        self.logger.info(f"ineligible users after before special memberships: [{len(ineligible_users)}]")

        super().save_document_version(ineligible_users, pipeline_config['ineligible_users'])

        return super().update_pipeline(pipeline_config)
