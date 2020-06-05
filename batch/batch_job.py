import datetime
import time
import logging

import azure.batch.models as batch_models


class BatchJob(object):
    """Azure Batch Job

    Attributes:
        batch_client: the BatchServiceClient to use.
        job_id: the ID for the job.
        pool_id: the ID for the pool running the job.
        max_retries: max number of times failed tasks will attempt to rerun
    """

    def __init__(self, batch_client, job_id, pool_id, max_retries=1):
        self.job_id = job_id
        self.batch_client = batch_client
        self.pool_id = pool_id
        self.max_retries = max_retries

    @staticmethod
    def _calculate_task_time_threshold(timeout, current_time):
        """Calculate task time threshold

        Args:
            timeout: The maximum time to wait (in seconds). -1 means maximum task lifetime
                https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#other-limits
            current_time: datetime object; ex) datetime.datetime.now()

        Returns:
            Time to timeout at
        """
        if timeout == -1:
            # set to maximum task lifetime
            timeout = datetime.timedelta(days=7)
        else:
            timeout = datetime.timedelta(seconds=timeout)
        return current_time + timeout

    def get_cloud_job_info_from_azure(self):
        """Queries Azure to get CloudJob for job.

        If no job is found on Azure then returns None.

        Returns:
            CloudJob object.
        """
        return self.batch_client.job.get(self.job_id)

    def wait_for_all_tasks_state(self, state, timeout=-1):
        """Waits for all tasks in job to reach any specified state in set

        Args:
            state: TaskState enum ('active', 'completed', 'preparing', 'running')
            timeout: The maximum time to wait (in seconds). -1 means maximum task lifetime
                https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#other-limits

        Returns:
            success: Boolean. True if tasks reached desired state within timeout, False otherwise.
        """

        time_to_timeout_at = BatchJob._calculate_task_time_threshold(timeout, datetime.datetime.now())

        while datetime.datetime.now() < time_to_timeout_at:
            tasks = self.batch_client.task.list(self.job_id)
            tasks_not_state = [task for task in tasks if task.state != state]
            if not tasks_not_state:
                return True
            logging.info("Waiting for {} tasks for job [{}] to reach state {}...".format(
                len(tasks_not_state), self.job_id, state))
            time.sleep(20)

        return False

    def wait_for_any_task_state(self, state, timeout=-1):
        """Waits for any task in job to reach a specified state in set

        Args:
            state: TaskState enum ('active', 'completed', 'preparing', 'running')
            timeout: The maximum time to wait (in seconds). -1 means maximum task lifetime
                https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#other-limits

        Returns:
            success: Boolean. True if at least one task has reached the desired state within timeout, False otherwise.
        """

        time_to_timeout_at = BatchJob._calculate_task_time_threshold(timeout, datetime.datetime.now())

        while datetime.datetime.now() < time_to_timeout_at:
            tasks = self.batch_client.task.list(self.job_id)
            if any(task.state == state for task in tasks):
                return True
            logging.info("Waiting for at least one task for job [{}] to reach state {}...".format(self.job_id, state))
            time.sleep(10)
        return False

    def get_failed_tasks(self):
        """Check for task failure and logs it while monitoring the job's state

        Returns:
            List of failed task commands to re-run
        """

        failures = set()
        tasks = self.batch_client.task.list(self.job_id)
        for task in tasks:
            if task.execution_info.result == batch_models.TaskExecutionResult.failure:
                logging.error("Task [{}] failed for job [{}] with message {}".format(task.id, self.job_id,
                                                                                     task.execution_info.failure_info))
                failures.add(task.id)
        return self._get_failed_task_commands(failures)

    def wait_for_job_prep_task(self, timeout=-1):
        """Waits for the job preparation task to finish running

        Args:
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if job prep task finished within timeout, False otherwise.
        """

        if timeout == -1:
            while True:
                logging.info("Waiting for job preparation task to complete for job [{}]...".format(self.job_id))
                status_list = list(self.batch_client.job.list_preparation_and_release_task_status(self.job_id))
                if all(status.job_preparation_task_execution_info.state ==
                       batch_models.JobPreparationTaskState.completed for status in status_list):
                    return True
                time.sleep(15)
        else:
            time_to_timeout_at = datetime.datetime.now() + datetime.timedelta(timeout)
            while True:
                logging.info("Waiting for job preparation task to complete for job [{}]...".format(self.job_id))
                status_list = list(self.batch_client.job.list_preparation_and_release_task_status(self.job_id))
                if datetime.datetime.now() < time_to_timeout_at:
                    if all(status.job_preparation_task_execution_info.state ==
                           batch_models.JobPreparationTaskState.completed for status in status_list):
                        return True
                    time.sleep(15)
                else:
                    return False

    def get_state(self):
        """Returns the current job state.

        Returns:
            state: String ('active', 'disabling', 'disabled', 'enabling', 'terminating', 'completed', 'deleting')
        """
        return self.get_cloud_job_info_from_azure().state

    def create_job(self, job_preparation_commands=None):
        """Creates a job with the specified ID, associated with the specified pool.

        Args:
            job_preparation_commands: commands as list of strings to run before the job starts

        Returns:
            success: True if job could be created successfully, False otherwise.
        """

        if job_preparation_commands is None:
            job = batch_models.JobAddParameter(id=self.job_id,
                                               pool_info=batch_models.PoolInformation(pool_id=self.pool_id))
        else:
            job_prep_task = batch_models.JobPreparationTask(command_line=job_preparation_commands,
                                                            wait_for_success=True,
                                                            rerun_on_node_reboot_after_success=True)
            job = batch_models.JobAddParameter(id=self.job_id,
                                               pool_info=batch_models.PoolInformation(pool_id=self.pool_id),
                                               job_preparation_task=job_prep_task)

        try:
            logging.info('Attempting to create job [{}]...'.format(self.job_id))
            self.batch_client.job.add(job)
            logging.info('Job [{}] created successfully...'.format(self.job_id))
            return True
        except batch_models.batch_error.BatchErrorException as err:
            if err.error.code == "JobExists":
                logging.info("Job [{}] already exists".format(self.job_id))
                return False
            else:
                logging.exception("Unknown error occurred while trying to create job [{}]".format(self.job_id))
                raise

    def add_task_collection(self, commands):
        """Add tasks to job

        Args:
            commands: command line commands as list of strings to run as part of the job

        Returns:
            success: Boolean. True if at least one task starts running, False otherwise
        """

        for idx, command in enumerate(commands):
            self.batch_client.task.add(self.job_id, batch_models.TaskAddParameter(
                id="{}-{}-{}".format(self.job_id, idx, datetime.datetime.now().strftime("%Y%m%d%H%M%S")),
                command_line=command
            ))

        # let job prep task finish if exists, wait for tasks to start running
        # state goes from running, to preparing, to running if job prep task exists
        logging.info("Waiting for tasks to start running [{}]...".format(self.job_id))
        try:
            _ = list(self.batch_client.job.list_preparation_and_release_task_status(self.job_id))
            self.wait_for_any_task_state(batch_models.TaskState.preparing)
        except batch_models.batch_error.BatchErrorException as err:
            if err.error.code == "JobPreparationTaskOrReleaseTaskNotSpecified":
                pass
            else:
                logging.exception("Unknown error occurred while the job "
                                  "preparation status for [{}]".format(self.job_id))
                raise
        return self.wait_for_any_task_state(batch_models.TaskState.running)

    def _get_failed_task_commands(self, failures):
        """Gets a list of commands for failed tasks to retry and removes the original ones

        Args:
            failures: list of failed task ids

        Returns:
            List of commands or False
        """

        if not failures:
            logging.info("There are no failed tasks for job [{}] to re-run".format(self.job_id))
            return False
        if self.max_retries == 0:
            logging.error("The number of re-tries to run failed tasks has reached the specified limit"
                          " for job [{}], stopping operation".format(self.job_id))
            return False
        self.max_retries = self.max_retries - 1
        commands = []
        for task_id in failures:
            commands.append(self.batch_client.task.get(self.job_id, task_id).command_line)
            self.batch_client.task.delete(self.job_id, task_id)
        return commands
