import json
import logging
import sys

from logging.handlers import RotatingFileHandler
from os import path, makedirs

from dotenv import load_dotenv
from distribution_tasks import *
from distribution_tasks.distribution_task import DistributionTask

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # load environment variables
    load_dotenv()

    # load config
    with open(path.normpath("config.json"), 'r') as f:
        config = json.load(f)

    # create round_{} folder so that we can put the log in there
    base_dir = path.dirname(path.abspath(__file__))
    log_dir = path.join(base_dir, f"./out/round_{int(sys.argv[1])}/logs")
    makedirs(path.normpath(log_dir), exist_ok=True)

    # set up logging
    formatter = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - %(message)s")
    logger_name = "donut-bot-distribution"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(path.normpath(path.join(log_dir, f"distribution_{int(sys.argv[1])}.log")), maxBytes=2500000, backupCount=4)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("searching project for tasks...")

    tasks = []
    global_objs = list(globals().items())

    for subclass in DistributionTask.__subclasses__():
        tasks.append(subclass(config, "donut-bot-distribution"))

    tasks = [task for task in tasks if task.priority >= 0]
    tasks.sort(key=lambda x: x.priority)

    logger.info(f"{len(tasks)} tasks found")
    logger.info("begin processing task pipeline...")

    pipeline_config = {'step': 0, 'round': int(sys.argv[1])}
    for task in tasks:
        pipeline_config = task.process(pipeline_config)
        pipeline_config['step'] += 1

    logger.info("task pipeline complete")
