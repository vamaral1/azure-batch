import logging
import time
import datetime

import azure.batch.models as batch_models


class BatchPool(object):
    """Azure Batch Pool

    Attributes:
        batch_client: the BatchServiceClient to use
        pool_id: ID for the new pool
    """

    def __init__(self, batch_client, pool_id):
        self.batch_client = batch_client
        self.pool_id = pool_id

    @staticmethod
    def _create_start_task(command, resource_files):
        """The start task is run each node as it joins the pool, and when it's rebooted or re-imaged to prep for
        any jobs later. The script is run "shared" directory that all tasks that run on the node have access to.

        Args:
            command: a string containing all the commands to run as the start task
            resource_files: list of file references (of type azure.batch.models.ResourceFile)
                from Azure Storage to download to each node

        Returns:
            azure.batch.models.StartTask object
        """

        user = batch_models.AutoUserSpecification(
            scope=batch_models.AutoUserScope.pool,
            elevation_level=batch_models.ElevationLevel.admin)
        return batch_models.StartTask(
                command_line=command,
                user_identity=batch_models.UserIdentity(auto_user=user),
                wait_for_success=True,
                resource_files=resource_files
            )

    def _select_latest_verified_vm_image_with_node_agent_sku(self, publisher, offer, sku):
        """Select the latest verified image that Azure Batch supports given
        a publisher, offer and sku.
        See https://docs.microsoft.com/en-us/azure/batch/batch-linux-nodes

        Args:
            publisher: VM image publisher (ex Canonical)
            offer: VM image offer (ex UbuntuServer)
            sku: VM image sku (ex 16.04)

        Returns:
            tuple: (Node agent sku id to use, VM image ref to use)
        """

        # get verified vm image list and node agent sku ids from service
        node_agent_skus = self.batch_client.account.list_node_agent_skus()
        # pick the latest supported sku
        skus_to_use = [
            (_sku, image_ref) for _sku in node_agent_skus for image_ref in sorted(
                _sku.verified_image_references, key=lambda item: item.sku)
            if image_ref.publisher.lower() == publisher.lower() and
               image_ref.offer.lower() == offer.lower() and
               image_ref.sku.startswith(sku)
        ]
        # skus are listed in reverse order, pick first for latest
        sku_to_use, image_ref_to_use = skus_to_use[0]
        return sku_to_use.id, image_ref_to_use

    def create_pool(self, dedicated_node_count, low_priority_node_count, num_cores_per_node,
                    pool_vm_size, publisher, offer, sku, start_task, resource_files):
        """Starts pool creation on Azure
        Args:
            dedicated_node_count: Number of dedicated nodes in the pool
            low_priority_node_count: number of low priority nodes in pool
            num_cores_per_node: Number of cores per node; depends on the pool_vm_size
            pool_vm_size: The type of VM to run on each node;
                see https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs
            publisher: VM image publisher (ex Canonical)
            offer: VM image offer (ex UbuntuServer)
            sku: VM image sku (ex 16.04)
            start_task: command to run when each node joins the bool formatted as a string
            resource_files: list of file references (of type azure.batch.models.ResourceFile)
                from Azure Storage to download to each node

        Returns:
            True if creation could be started successfully. False if pool already exists.
            Throws error if something else went wrong.
        """

        sku_to_use, image_ref_to_use = self._select_latest_verified_vm_image_with_node_agent_sku(publisher, offer, sku)
        new_pool = batch_models.PoolAddParameter(
            id=self.pool_id,
            virtual_machine_configuration=batch_models.VirtualMachineConfiguration(
                image_reference=image_ref_to_use,
                node_agent_sku_id=sku_to_use),
            vm_size=pool_vm_size,
            target_dedicated_nodes=dedicated_node_count,
            target_low_priority_nodes=low_priority_node_count,
            start_task=self._create_start_task(start_task, resource_files),
            max_tasks_per_node=num_cores_per_node,
            task_scheduling_policy=batch_models.TaskSchedulingPolicy(batch_models.ComputeNodeFillType.spread)
        )

        try:
            logging.info("Attempting to create pool [{}]...".format(self.pool_id))
            self.batch_client.pool.add(new_pool)
            logging.info("Pool [{}] created successfully...".format(self.pool_id))
            return True
        except batch_models.BatchErrorException as e:
            if e.error.code == "PoolExists":
                logging.info("Pool [{}] already exists".format(self.pool_id))
                return False
            else:
                logging.exception("Unknown error occurred while trying to create pool [{}]".format(self.pool_id))
                raise

    def get_cloud_pool_info_from_azure(self):
        """Queries Azure to get CloudPool for pool.

        If no pool is found on Azure then returns None.

        Returns:
            CloudPool object.
        """
        return self.batch_client.pool.get(self.pool_id)

    def get_state(self):
        """Returns the current pool state.

        Returns:
            state: enum PoolState ('active', 'upgrading', 'deleting', or '' otherwise)
        """
        return self.get_cloud_pool_info_from_azure().state

    def _wait_until_state(self, state, timeout=-1):
        """Wait until state is reached or timeout over.

        Params:
            state: enum PoolState ('active', 'upgrading', 'deleting')
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if state was reached within timeout, False otherwise.
        """
        if timeout == -1:
            while self.get_state() != state:
                time.sleep(10)
        else:
            time_to_timeout_at = datetime.datetime.now() + datetime.timedelta(timeout)
            while self.get_state() != state:
                if datetime.datetime.now() < time_to_timeout_at:
                    time.sleep(10)
                else:
                    return False
        return True

    def _wait_until_not_state(self, state, timeout=-1):
        """Wait until state is NOT passed state or timeout over.

        Params:
            state: enum PoolState ('active', 'upgrading', 'deleting')
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if state is not passed state within timeout, False otherwise.
        """
        if timeout == -1:
            while self.get_state() == state:
                time.sleep(10)
        else:
            time_to_timeout_at = datetime.datetime.now() + datetime.timedelta(timeout)
            while self.get_state() == state:
                if datetime.datetime.now() < time_to_timeout_at:
                    time.sleep(10)
                else:
                    return False
        return True

    def wait_for_deletion(self, timeout=-1):
        """Wait until pool is done being deleted or timeout is up.

        Args:
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if pool deletion was completed within timeout, False otherwise.
        """

        if self.get_state() != batch_models.PoolState.deleting:
            raise ValueError("The current pool state is not 'deleting', please delete pool [{}] first")
        try:
            self._wait_until_not_state(batch_models.PoolState.deleting, timeout=timeout)
        except batch_models.BatchErrorException as e:
            if e.error.code == "PoolNotFound":
                return True
            else:
                logging.exception("Unknown error occurred while waiting pool [{}] deletion to complete".format(self.pool_id))
                raise
        return False

    def _are_all_nodes_state(self, state):
        """Checks if nodes have reached a desired state

        Args:
            state: ComputeNodeState enum ('creating', 'idle', 'leaving_pool', 'offline', 'preempted', 'rebooting',
                'reimaging', 'running', 'start_task_failed', 'starting', 'unknown', 'unusable', 'waiting_for_start_task')

        Returns:
            success: Boolean. True if nodes reached desired state, False otherwise.
        """

        # refresh pool to ensure that there is no resize error
        pool = self.batch_client.pool.get(self.pool_id)
        if pool.resize_errors is not None:
            resize_errors = "\n".join([repr(e) for e in pool.resize_errors])
            raise RuntimeError('Resize error encountered for pool [{}]:\n{}'.format(self.pool_id, resize_errors))
        nodes = list(self.batch_client.compute_node.list(self.pool_id))
        if (len(nodes) >= pool.target_dedicated_nodes) and all(node.state == state for node in nodes):
            return True
        return False

    def wait_for_all_nodes_state(self, state, timeout=-1):
        """Waits for all nodes in pool to reach any specified state in set

        Args:
            state: ComputeNodeState enum ('creating', 'idle', 'leaving_pool', 'offline', 'preempted', 'rebooting',
                'reimaging', 'running', 'start_task_failed', 'starting', 'unknown', 'unusable', 'waiting_for_start_task')
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if nodes reached desired state within timeout, False otherwise.
        """

        if timeout == -1:
            while not self._are_all_nodes_state(state):
                logging.info('Waiting for all nodes in pool [{}] to reach state {!r}'.format(self.pool_id, state))
                time.sleep(15)
        else:
            time_to_timeout_at = datetime.datetime.now() + datetime.timedelta(timeout)
            while not self._are_all_nodes_state(state):
                if datetime.datetime.now() < time_to_timeout_at:
                    logging.info('Waiting for all nodes in pool [{}] to reach state {!r}'.format(self.pool_id, state))
                    time.sleep(15)
                else:
                    return False
        return True

    def wait_for_all_nodes_not_state(self, state, timeout=-1):
        """Waits for all nodes in pool to leave any specified state in set

        Args:
            state: ComputeNodeState enum ('creating', 'idle', 'leaving_pool', 'offline', 'preempted', 'rebooting',
                'reimaging', 'running', 'start_task_failed', 'starting', 'unknown', 'unusable', 'waiting_for_start_task')
            timeout: The maximum time to wait (in seconds). -1 means no limit.

        Returns:
            success: Boolean. True if nodes left desired state within timeout, False otherwise.
        """

        if timeout == -1:
            while self._are_all_nodes_state(state):
                logging.info('Waiting for all nodes in pool [{}] to leave state {!r}'.format(self.pool_id, state))
                time.sleep(15)
        else:
            time_to_timeout_at = datetime.datetime.now() + datetime.timedelta(timeout)
            while self._are_all_nodes_state(state):
                if datetime.datetime.now() < time_to_timeout_at:
                    logging.info('Waiting for all nodes in pool [{}] to leave state {!r}'.format(self.pool_id, state))
                    time.sleep(15)
                else:
                    return False
        return True

    def reboot_pool(self):
        """Reboots all the nodes in the pool

        Returns:
            success: Boolean. True if nodes rebooted, False otherwise.
        """

        nodes = list(self.batch_client.compute_node.list(self.pool_id))
        for node in nodes:
            self.batch_client.compute_node.reboot(pool_id=self.pool_id, node_id=node.id)
        return self.wait_for_all_nodes_state(batch_models.ComputeNodeState.idle)

    def update_pool(self, new_command, new_resource_files):
        """Updates the pool with a new start task command and resource files

        Args:
            new_command: command to run when each node joins the bool formatted as a string
            new_resource_files: list of file references (of type azure.batch.models.ResourceFile)
                from Azure Storage to download to each node

        Returns:
            success: Boolean. True if nodes update and reboot, False otherwise.
        """

        logging.info("Updating pool [{}] with new start task".format(self.pool_id))
        self.wait_for_all_nodes_not_state(batch_models.ComputeNodeState.running)
        self.batch_client.pool.patch(self.pool_id, pool_patch_parameter=batch_models.PoolPatchParameter(
            start_task=BatchPool._create_start_task(new_command, new_resource_files)))
        return self.reboot_pool()

