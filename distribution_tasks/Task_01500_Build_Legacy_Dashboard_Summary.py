import json
import os
from decimal import Decimal

from web3 import Web3
from distribution_tasks.distribution_task import DistributionTask

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from safe import safe_tx, safe_tx_builder
from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json


class BuildLegacySummaryDistributionTask(DistributionTask):

    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 1500

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        distribution_summary = super().get_current_document_version(pipeline_config['distribution_summary'])

        summary = {}

        for d in distribution_summary:
            summary.update({
                d['username']: {
                    'username': d['username'],
                    'address': d['address'],
                    'donut': float(d['points']),
                    'data': {
                        'removed': d['eligible'] == 'False',
                        'removalReason': d['eligibility_reason'],
                        'fromKarma': float(d['base']),
                        'fromTipsGiven': 0,
                        'fromTipsRecd': float(d['quad_rank']),
                        'voterBonus': float(d['voting']),
                        'pay2PostFee': abs(float(d['pay2post']))
                    }
                }
            })

        json_structure = {
            'label': f"round_{super().distribution_round}",
            'totalDistribution': sum([float(x['points']) for x in distribution_summary]),
            'pay2post': 0,
            'totalVoterBonus': sum([float(x['voting']) for x in distribution_summary]),
            'totalFromRemovedUsers': 0,
            'summary': summary
        }

        tx_builder_dir = self.pipeline['legacy_dir']
        tx_file_path = os.path.join(tx_builder_dir, f"distributionSummary.json")

        if os.path.exists(tx_file_path):
            os.remove(tx_file_path)

        with open(tx_file_path, 'w') as f:
            json.dump(json_structure, f, indent=4)

        return super().update_pipeline(pipeline_config)
