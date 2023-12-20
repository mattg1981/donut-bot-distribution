import csv
import importlib
import json
import logging
import os
import re
import shutil
from pathlib import Path


class DistributionTask:
    config = {}

    def save_document_version(self, obj, filename: str) -> str:
        """
        Save the passed in object to a versioned file
        """
        if not obj:
            return None

        file_names = [file for file in os.listdir(self.working_directory)
                      if file.startswith(f"{filename}.")]

        version = 0

        regex_pattern = f'{filename}\\.(\\d*)\\.\\w+\\.csv'
        regex = re.compile(regex_pattern)

        if len(file_names):
            for file in file_names:
                regex_result = regex.search(file)
                if int(regex_result[1]) > version:
                    version = int(regex_result[1])

            version += 1

        file_location = os.path.join(self.working_directory,
                                f"{filename}.{str(version).zfill(3)}.{self._logger_extra}.csv")

        if isinstance(obj, list) and not isinstance(obj[0], dict):
            # attempt to turn the obj into a dict (should work with classes but will likely not work with all
            # objects being passed in and will need to be tweaked to handle those.  The only 2 things I pass in
            # currently are dicts and objects, so it works for our needs now
            try:
                obj = [vars(o) for o in obj]
            except Exception:
                return None

        with open(file_location, 'w') as output_file:
            writer = csv.DictWriter(output_file, obj[0].keys(), extrasaction='ignore')
            writer.writeheader()
            writer.writerows(obj)

        return file_location

    def get_current_document_version(self, filename):
        directories = [self.cache_directory, self.working_directory]

        max_version = 0
        max_versioned_location = None
        top_level_dir = None

        for dir in directories:
            file_names = [file for file in os.listdir(dir) if file.startswith(f"{filename}.")]

            if not file_names:
                continue

            regex_pattern = f'{filename}\\.(\\d*)\\.\\w+\\.csv'
            regex = re.compile(regex_pattern)

            if not max_versioned_location:
                max_versioned_location = file_names[0]
                top_level_dir = dir

            for file in file_names:
                regex_result = regex.search(file)
                if int(regex_result[1]) > max_version:
                    max_version = int(regex_result[1])
                    max_versioned_location = file
                    top_level_dir = dir

        if not max_versioned_location:
            return None

        location = os.path.join(top_level_dir, max_versioned_location)

        with open(location, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            return list(reader)

    def get_document_version(self, filename, version="latest"):
        if version == "latest":
            return self.get_current_document_version(filename)

        file_names = [file for file in os.listdir(self.working_directory)
                      if file.startswith(f"{filename}.")]

        regex_pattern = f'{filename}\.(\d*)\.\w+\.csv'
        regex = re.compile(regex_pattern)

        file_version = None

        for file in file_names:
            regex_result = regex.search(file)
            if int(regex_result[1]) == version:
                file_version = file

        location = os.path.join(self.working_directory, file_version)

        with open(location, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            return list(reader)

    def save_safe_tx(self, tx):
        tx_builder_dir = self.pipeline['tx_builder_dir']
        tx_file_path = os.path.join(tx_builder_dir,f"tx_{self.distribution_round}.json")

        if os.path.exists(tx_file_path):
            os.remove(tx_file_path)

        with open(tx_file_path, 'w') as f:
            json.dump(tx, f, indent=4)

    def cache_file(self, versioned_document_path):
        basename = os.path.basename(versioned_document_path)
        shutil.copyfile(versioned_document_path, os.path.join(self.cache_directory, basename))

    @property
    def distribution_round(self):
        if not self.pipeline:
            return None

        return self.pipeline['round']

    @property
    def current_step(self):
        if not self.pipeline:
            return None

        return self.pipeline['step']

    @property
    def working_directory(self):
        if not self.pipeline:
            return None

        return self.pipeline['working_dir']

    @property
    def cache_directory(self):
        if not self.pipeline:
            return None

        return self.pipeline['cache_dir']

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value):
        self._priority = value

    def __init__(self, config, logger_name):
        self._priority = None

        self.pipeline = None
        self.config = config
        self.logger = logging.getLogger(logger_name)

        child = importlib.import_module(self.__module__).__file__
        child = Path(child).stem
        child = child[0:10]
        self._logger_extra = child.lower()

    def process(self, pipeline_config):
        self.pipeline = pipeline_config
        return

    def update_pipeline(self, pipeline, items=None):
        if items:
            pipeline.update(items)
        self.pipeline = pipeline
        return pipeline
