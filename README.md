## Python modules for interacting with Azure Batch

[Azure Batch](https://docs.microsoft.com/en-us/azure/batch/) follows the paradigm of infrastructure as code. It contains a Python API that lets users build workflows to run large-scale applications efficiently in the cloud, dynamically adjusting resources as needed. This repository builds on top of the Python API to faciliate scheduling, executing, and managing tasks.

The main data processing workflow is as follows: 
* upload input data files to Azure Storage and create a `pool` of Batch compute nodes (virtual machines)
* create a `job` that runs `tasks` to process each input file in the `pool` using a basic command

`BatchProcessor` - manages the creation and deletion of a `pool`, executes and monitors `job`s
`BatchPool` - facilitates the configuration, creation, and mangement of a `pool` of VMs.
`BatchJob` - creates jobs, adds `tasks` to the `job`, monitors the `job` and queues failed jobs for retry at a later time. A `task` is simply a Linux command line command.