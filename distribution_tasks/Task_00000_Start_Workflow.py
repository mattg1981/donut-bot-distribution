import glob
import os

from os import path
from distribution_tasks.distribution_task import DistributionTask


class CreateDirectoryStructureDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)
        self.priority = 0

    def process(self, pipeline_config):
        super().process(pipeline_config)
        self.logger.info(f"begin task [step: {super().current_step}] [file: {os.path.basename(__file__)}]")

        self.logger.info("  build directory structure...")

        # create file structure
        base_dir = path.dirname(path.abspath(__file__))
        working_dir = path.join(base_dir, f"../out/round_{self.distribution_round}")
        cache_dir = path.join(base_dir, f"../cache/round_{self.distribution_round}")

        os.makedirs(path.normpath(working_dir), exist_ok=True)
        os.makedirs(path.normpath(cache_dir), exist_ok=True)

        self.logger.info("  clear out workding directory...")

        # clear out the working directory (in case files remain from a previous run)
        files = glob.glob(f'{working_dir}/*.csv')
        for f in files:
            os.remove(f)

        locations = [path.join(working_dir, "logs"), path.join(working_dir, "tx_builder")]

        for location in locations:
            os.makedirs(location, exist_ok=True)
            os.makedirs(location, exist_ok=True)

        return super().update_pipeline(pipeline_config, {
            'working_dir': os.path.normpath(working_dir),
            'cache_dir': os.path.normpath(cache_dir),
            'log_dir': path.join(working_dir, "logs"),
            'tx_builder_dir': path.join(working_dir, "tx_builder")
        })
