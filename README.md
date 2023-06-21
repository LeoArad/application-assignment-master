# application-assignment

## Starting the service:
To start the service, navigate to the `application-assignment-master` folder and run the `docker-compose up` command. This will create an environment with the following containers: `consumer`, `publisher`, `rabbitmq`, and `postgresdb`.

The `consumer` and `publisher` containers are Python packages that will be built based on the `setup.py` file and will run tests from each package's test folder.

The `consumer` service is a combination of a RabbitMQ consumer that consumes messages sent from the `publisher` service. It stores the data into the `postgresdb` `meds` table. Additionally, it acts as an API server listening on port 8112. It returns medicine information for a given patient ID.

To retrieve medicine information for a specific patient ID, make the following request:
http://{server_address}:8112/{p_id}

Please replace `{server_address}` with the appropriate address where the service is running, and `{patient_id}` with the desired patient ID.

If you have any further questions or need assistance, feel free to ask!

