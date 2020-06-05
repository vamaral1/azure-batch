import os
from datetime import date
import configparser
from dotenv import load_dotenv, find_dotenv
import logging
from itertools import chain

import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batch_models

from batch.batch_pool import BatchPool
from batch.batch_job import BatchJob

logging.basicConfig(format="%(asctime)s: %(levelname)s: %(module)s: %(funcName)s: %(message)s", level="INFO")


class BatchProcessor(object):
    """Batch Processor for the Azure Batch service in a specified region where the service is available
    Creates pools of Ubuntu machines that can run any assigned task.

    Some code was adapted from https://github.com/Azure/azure-batch-samples/tree/master/Python/Batch
    Here's a guide https://docs.microsoft.com/en-us/azure/batch/batch-api-basics

    Requires config.cfg file (specifies config for Batch) and .env file (includes keys for Azure services)

    Example:

    batch_processor = BatchProcessor()

    batch_processor.create_pool(pool_id=pool_id,...,resource_files=resource_files)
    batch_processor.create_job(job_id, pool_id)
    batch_processor.run_tasks(job_id, tasks)
    """

    def __init__(self):
        load_dotenv(find_dotenv())
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'config.cfg'))

        self.region = config.get('DEFAULT', 'region')
        self.node_os_publisher = config.get('DEFAULT', "node_os_publisher")
        self.node_os_offer = config.get('DEFAULT', "node_os_offer")
        self.node_os_sku = config.get('DEFAULT', "node_os_sku")
        self.pools_quota = config.getint('DEFAULT', 'pools_quota')
        self.jobs_quota = config.getint('DEFAULT', 'jobs_quota')
        self.dedicated_cores_available = config.getint('DEFAULT', 'dedicated_cores_quota')
        self.low_priority_cores_available = config.getint('DEFAULT', 'low_priority_cores_quota')

        self.batch_client = batch.BatchServiceClient(
            credentials=batch_auth.SharedKeyCredentials(os.getenv("BATCH_ACCOUNT"),
                                                        os.getenv("BATCH_PRIMARY_ACCESS_KEY")),
            base_url=os.getenv("BATCH_URL"))

        self.jobs = {}
        self.pools = {}

    @staticmethod
    def wrap_commands_in_shell(os_type, commands):
        """Wrap commands in a shell for execution

        Args:
            os_type: OS type, linux or windows
            commands: list of commands to execute

        Returns:
            Formatted string of commands depending on OS
        """

        if os_type.lower() == 'linux':
            shell_str = "/bin/bash -c \'set -e; set -o pipefail"
            if len(commands) == 1 and commands[0].startswith(shell_str):
                return commands[0]
            return '{}; {}; wait\''.format(shell_str, ';'.join(commands))
        elif os_type.lower() == 'windows':
            shell_str = "cmd.exe /c"
            if len(commands) == 1 and commands[0].startswith(shell_str):
                return commands[0]
            return '{} "{}"'.format(shell_str, '&'.join(commands))
        else:
            raise ValueError('Unknown OS type: {}'.format(os_type))

    def create_pool(self, pool_id, pool_node_count, num_cores_per_node, pool_vm_size, start_task, resource_files):
        """Starts pool creation on Azure

        Args:
            pool_id: ID for the new pool
            pool_node_count: Number of nodes in the pool
            num_cores_per_node: Number of cores per node; depends on the pool_vm_type
            pool_vm_size: The type of VM to run on each node;
                see https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs
            start_task: list of commands to run for each node joining the pool
            resource_files: list of file references (of type azure.batch.models.ResourceFile)
                from Azure Storage to download to each node

        Returns:
            success: Boolean. True if creation could be started successfully, False otherwise.
        """

        if len(self.pools) == self.pools_quota:
            raise BufferError('Pool {} cannot be added to Azure Batch Service since '
                              'the pool quota of {} has been reached'.format(pool_id, self.pools_quota))
        if pool_node_count*num_cores_per_node > self.dedicated_cores_available + self.low_priority_cores_available:
            raise ValueError('The total number of cores specified is greater than the quota')
        start_task = BatchProcessor.wrap_commands_in_shell('linux', start_task)
        dedicated_node_count = min(pool_node_count, int(self.dedicated_cores_available/num_cores_per_node))
        low_priority_node_count = min(pool_node_count - dedicated_node_count,
                                      int(self.low_priority_cores_available/num_cores_per_node))
        pool = BatchPool(self.batch_client, pool_id)
        self.pools[pool_id] = pool
        created = pool.create_pool(dedicated_node_count, low_priority_node_count, num_cores_per_node, pool_vm_size,
                                   self.node_os_publisher, self.node_os_offer, self.node_os_sku,
                                   start_task, resource_files)
        if created:
            logging.info("Waiting for pool [{}] nodes to start and run start task".format(pool_id))
            start_tasks_finished = pool.wait_for_all_nodes_state(batch_models.ComputeNodeState.idle)
            if not start_tasks_finished:
                raise TimeoutError("Timed out waiting for pool [{}] to finish running start task".format(pool_id))
            logging.info("Pool [{}] is now ready to run a job".format(pool_id))
            return True
        return False

    def create_job(self, job_id, pool_id, job_preparation_task=None):
        """Creates a job with the specified ID, associated with the specified pool.

        Args:
            job_id: the ID for the job.
            pool_id: The ID for the pool.
            job_preparation_task: list of commands to run for each node before the tasks run

        Returns:
            success: Boolean. True if creation could be started successfully, False otherwise.
        """

        if len(self.jobs) == self.jobs_quota:
            raise BufferError('Job {} cannot be added to Azure Batch Service since '
                              'the jobs quota of {} has been reached'.format(job_id, self.jobs_quota))
        elif pool_id not in self.pools:
            raise KeyError("Pool {} does not exist, please create it first before "
                           "creating an associated job".format(pool_id))

        job = BatchJob(self.batch_client, job_id, pool_id)
        self.jobs[job_id] = job
        if job_preparation_task is None:
            created = job.create_job()
        else:
            created = job.create_job(BatchProcessor.wrap_commands_in_shell('linux', job_preparation_task))
            job.wait_for_job_prep_task()
        if created:
            return True
        return False

    def run_tasks(self, job_id, tasks):
        """Add tasks to a job

        Args:
            job_id: the ID for the job.
            tasks: list of commands as strings to run.

        Returns:
            success: True if the tasks finished running, False otherwise.
        """

        if job_id not in self.jobs:
            raise KeyError("Job {} does not exist, please create it first before "
                           "adding associated tasks".format(job_id))
        job = self.jobs[job_id]
        job.add_task_collection(BatchProcessor.wrap_commands_in_shell('linux', [task]) for task in tasks)
        logging.info("Running tasks for job [{}]...".format(job_id))
        completed = job.wait_for_all_tasks_state(batch_models.TaskState.completed)
        if not completed:
            raise TimeoutError("Timed out waiting for tasks to complete for job [{}]".format(job_id))
        logging.info("Tasks for job [{}] have finished running...".format(job_id))
        failed_tasks = job.get_failed_tasks()
        if failed_tasks:
            logging.info("Retrying failed tasks for job [{}]...".format(job_id))
            # reboots in case failed tasks left nodes in inconsistent state
            self.pools[job.pool_id].reboot_pool()
            return self.run_tasks(job_id, failed_tasks)
        else:
            return completed

    def delete_pool(self, pool_id):
        """Wrapper to delete a pool

        Args:
            pool_id: the ID for the pool to delete

        Returns:
            success: True if pool was deleted, False otherwise.
        """

        if pool_id in self.pools:
            self.batch_client.pool.delete(pool_id)
            logging.info("Deleting pool [{}]...".format(pool_id))
            self.pools[pool_id].wait_for_deletion()
            del self.pools[pool_id]
            logging.info("Pool [{}] has been successfully deleted".format(pool_id))
            return True
        else:
            logging.warning("Pool [{}] does not exist so it was not deleted".format(pool_id))
            return False

    def delete_job(self, job_id):
        """Wrapper to delete a job

        Args:
            job_id: the ID for the job to delete

        Returns:
            success: True if job was deleted, False otherwise.
        """

        if job_id in self.jobs:
            self.batch_client.job.delete(job_id)
            logging.info("Deleting job [{}]...".format(job_id))
            del self.jobs[job_id]
            logging.info("Job [{}] has been successfully deleted".format(job_id))
            return True
        else:
            logging.warning("Job [{}] does not exist so it was not deleted".format(job_id))
            return False

    def get_cloud_pool_info(self, pool_id):
        """Get information of current pool status

        Args:
            pool_id: the ID for the pool

        Returns:
            CloudPool object
        """

        return self.pools[pool_id].get_cloud_pool_info_from_azure()

    def get_cloud_job_info(self, job_id):
        """Get information of current job status

        Args:
            job_id: the ID for the job

        Returns:
            CloudJob object
        """

        return self.jobs[job_id].get_cloud_job_info_from_azure()

    def update_pool(self, pool_id, new_start_task, new_resource_files):
        """Updates the specified pool with a new start task command and resource files

        Args:
            pool_id: the ID for the pool
            new_start_task: list of commands to run for each node in the pool
            new_resource_files: list of file references (of type azure.batch.models.ResourceFile)
                from Azure Storage to download to each node

        Returns:
            success: Boolean. True if nodes update and reboot, False otherwise.
        """

        new_start_task = BatchProcessor.wrap_commands_in_shell('linux', new_start_task)
        return self.pools[pool_id].update_pool(new_start_task, new_resource_files)

    def download_output_logs(self, job_id):
        """Gets the stdout and stderr logs from all the tasks in the job

        Args:
            job_id: the ID for the job
        """

        tasks = self.batch_client.task.list(job_id)
        for task in tasks:
            std_out = self.batch_client.file.get_from_task(job_id, task.id, 'stdout.txt')
            std_err = self.batch_client.file.get_from_task(job_id, task.id, 'stderr.txt')
            if task.execution_info.result == batch_models.TaskExecutionResult.success:
                state = 'success'
            else:
                state = 'failed'
            BatchProcessor._read_stream_to_file(job_id, task.id, chain(std_out, std_err), state)

    @staticmethod
    def _read_stream_to_file(job_id, task_id, stream, state):
        """Takes a stream of data and writes to disk as a txt log

        Args:
            job_id: the ID for the job
            task_id: id of the task to write a log for
            stream: a bytes generator
            state: the state the task was left in. 'success' or 'failed'
        """

        timestamp = date.today().strftime("%Y%m%d")
        filename = os.path.join("logs", job_id, timestamp, state, task_id + ".txt")
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Download task output
        with open(filename, 'wb') as file_output:
            for data in stream:
                file_output.write(data)

