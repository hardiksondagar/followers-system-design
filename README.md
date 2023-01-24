# followers-system-design
## Overview
This system is designed to simulate the adding of followers in a social media platform. It uses RabbitMQ for message queueing and multiple PostgreSQL servers for data storage and retrieval. The system is designed to handle a large amount of user followers data, and allows for benchmarking of different configurations and setups.

The system includes a user followers database that is partitioned across multiple shards, with each shard containing a subset of the data. The system also includes a message queue, which is used to handle the flow of data between the different components of the system, such as the data generator, the data processor, and the database. The system also includes scripts for generating and processing data, as well as scripts for running basic queries on the database for testing and benchmarking purposes.

## settings.py
This file contains settings for the system setup. It should be reviewed and edited if necessary before running the system.

## setup.py
This file contains the necessary steps for setting up RabbitMQ for testing and benchmarking purposes, as well as for configuring multiple PostgreSQL servers to act as individual shards.

## db.py
This file contains common database operations that are used throughout the system.

## followers_publish.py
This script is used to simulate adding followers to users in a social media platform. It adds followers data into a RabbitMQ queue.

## followers_consume.py
This script consumes followers data from the queue and pushes it into the databases.

## queries.py
This file contains basic queries that can be run on the database shards for testing and benchmarking purposes.