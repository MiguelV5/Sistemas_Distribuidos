# rabbitmq-example

## Hello World Example
The following example is a dockerization of the [RabbitMQ Hello World!! example](https://www.rabbitmq.com/tutorials/tutorial-one-python.html). It consists in two processes:
* **Producer:** Process that constantly sends messages in queue _hello_ 
* **Consumer:** Process receives messages from queue _hello_ and prints them

The example run via **docker-compose** and can be executed with the script **run.sh**. The example needs to be stopped executing the script **stop.sh** which will free all docker-compose resources allocated 