from distribution_tasks.distribution_task import DistributionTask


class SampleDistributionTask(DistributionTask):
    def __init__(self, config, logger_name):
        DistributionTask.__init__(self, config, logger_name)

        # be sure to change the priority - this value determines
        # the order the task will be executed (smaller values have higher priority)
        self.priority = -1

    def process(self, pipeline_config):
        self.logger.info("begin task")

        # important - this call loads the configuration so that you can use properties such as:
        # super().distribution_round
        # super().working_dir
        super().process(pipeline_config)

        # perform task logic

        self.logger.info("end task")

        # add results to the pipeline result. These results can be accessed by other tasks that run after this one.
        return super().update_pipeline(pipeline_config, {
            'additional_key_to_be_passed_downtstream': 'value',
        })
