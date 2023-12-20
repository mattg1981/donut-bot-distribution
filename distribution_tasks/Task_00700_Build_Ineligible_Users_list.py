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
        self.priority = 700

    def process(self, pipeline_config):
        self.logger.info("begin task")
        super().process(pipeline_config)

        # load environment variables
        load_dotenv()

        distribution = super().get_current_document_version(pipeline_config['distribution'])
        distribution_round = super().get_current_document_version(pipeline_config['distribution_round'])

        distribution_round_end_date = datetime.strptime(distribution_round[0]['to_date'], '%Y-%m-%d %H:%M:%S.%f')
        cutoff_date = distribution_round_end_date - timedelta(days=60)

        # creating an authorized reddit instance
        reddit = praw.Reddit(client_id=os.getenv('INELIGIBLE_CLIENT_ID'),
                             client_secret=os.getenv('INELIGIBLE_CLIENT_SECRET'),
                             # username=os.getenv('INELIGIBLE_CLIENT_USERNAME'),
                             # password=os.getenv('INELIGIBLE_CLIENT_PASSWORD'),
                             user_agent="ethtrader ineligible-users (by u/mattg1981)")

        reddit.read_only = True

        # test for cached file
        ineligible_users_filename = "ineligible_users"
        ineligible_users = super().get_current_document_version(ineligible_users_filename)

        if not ineligible_users:
            ineligible_users = []

            idx = 0
            for d in distribution:
                idx += 1
                self.logger.info(f"checking eligiblity requirements for {d['username']} [{idx} of {len(distribution)}]")
                redditor = reddit.redditor(d['username'])

                try:
                    if hasattr(redditor, 'is_suspended'):
                        if redditor.is_suspended:
                            self.logger.info(f"  adding user [{d['username']}] to ineligible list: user is suspended")
                            ineligible_users.append({
                                'user': d['username'],
                                'reason': 'suspended'
                            })
                            continue

                    if redditor.total_karma < 100:
                        self.logger.info(f"  adding user [{d['username']}] to ineligible list: karma < 100")
                        ineligible_users.append({
                            'user': d['username'],
                            'reason': 'karma'
                        })
                        continue

                    if datetime.fromtimestamp(redditor.created) > cutoff_date:
                        self.logger.info(f"  adding user [{d['username']}] to ineligible list: created < 60 days")
                        ineligible_users.append({
                            'user': d['username'],
                            'reason': 'age'
                        })
                        continue

                    self.logger.info("  ok...")

                except prawcore.exceptions.NotFound as e:
                    self.logger.info(f"removing user [{d['username']}] from distribution: user is deleted (not found)")
                    self.logger.error(e)

                    ineligible_users.append({
                        'user': d['username'],
                        'reason': 'deleted'
                    })
                    continue

                except Exception as e:
                    self.logger.error(e)

        path = super().save_document_version(ineligible_users, ineligible_users_filename)
        super().cache_file(path)

        return super().update_pipeline(pipeline_config, {
            'ineligible_users': ineligible_users_filename
        })
