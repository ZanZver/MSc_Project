from locust import HttpUser, task, between, events


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Set default configurations for headless mode."""
    if environment.runner is None:  # Only applies in headless mode
        from locust.runners import Runner

        environment.runner = Runner(
            environment,
            {
                "host": "http://127.0.0.1:8000",
                "num_users": 100,  # Number of users
                "spawn_rate": 10,  # Spawn rate (users per second)
                "run_time": "5m",  # Duration
            },
        )


class BlockchainUser(HttpUser):
    wait_time = between(1, 3)  # Wait between tasks
    host = "http://127.0.0.1:8000"  # Base URL of the API

    task_executed = False  # Class-level flag for append_data
    delete_executed = False  # Class-level flag for delete_record

    @task
    def append_data(self):
        """Simulate POST request to append data to the blockchain."""
        if not BlockchainUser.task_executed:
            payload = {
                "key": "82HFE9767U326DEZ2",
                "key_field": "vin",
                "data": {
                    "vin": "82HFE9767U326DEZ2",
                    "license_plate": "WU37 WRN",
                    "vehicle_make": "Mitsubishi",
                    "vehicle_model": "Outlander",
                    "vehicle_year": 2000,
                    "full_vehicleInfo": {
                        "Year": 2000,
                        "Make": "Mitsubishi",
                        "Model": "Outlander",
                        "Category": "SUV",
                    },
                    "vehicle_category": "SUV",
                    "vehicle_make_model": "Mitsubishi Outlander",
                    "vehicle_year_make_model": "2000 Mitsubishi Outlander",
                    "vehicle_year_make_model_cat": "2000 Mitsubishi Outlander (SUV)",
                },
            }
            response = self.client.post("/blockchain/append-data", json=payload)
            if response.status_code != 200:
                print(f"Error: {response.status_code}, {response.text}")
            else:
                print(f"Data appended successfully: {response.json()}")
            BlockchainUser.task_executed = True  # Set the flag to prevent re-execution

    @task
    def delete_record(self):
        """Simulate DELETE request to delete a record from the blockchain."""
        if not BlockchainUser.delete_executed:
            params = {"key": "H1AUMH0D9M76R7NNG", "key_field": "vin"}
            response = self.client.delete("/blockchain/delete-record", params=params)
            if response.status_code != 200:
                print(f"Error: {response.status_code}, {response.text}")
            else:
                print(f"Record deleted successfully: {response.text}")
            BlockchainUser.delete_executed = (
                True  # Set the flag to prevent re-execution
            )

    @task
    def record_history(self):
        """Simulate GET request to fetch the history of a record."""
        params = {"key": "82HFE9767U326DEZ2", "key_field": "vin"}
        response = self.client.get("/blockchain/record-history", params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
        else:
            print(f"Record history retrieved successfully: {response.json()}")

    @task
    def get_latest_record(self):
        """Simulate GET request to fetch the latest record."""
        response = self.client.get(
            "/blockchain/latest-record",
            params={"key": "82HFE9767U326DEZ2", "key_field": "vin"},
        )
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")


class DBUser(HttpUser):
    wait_time = between(1, 3)  # Wait between tasks
    host = "http://127.0.0.1:8000"  # Base URL of the API

    update_executed = False  # Class-level flag for update_record
    delete_executed = False  # Class-level flag for delete_record

    @task
    def retrieve_all(self):
        """Simulate GET request to retrieve all records from the database."""
        response = self.client.get("/db/retrieve/all/")
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
        else:
            print(f"All records retrieved successfully: {response.json()}")

    @task
    def retrieve_specific(self):
        """Simulate GET request to retrieve a specific record from the database."""
        params = {"key": "H1AUMH0D9M76R7NNG"}
        response = self.client.get("/db/retrieve/specific/", params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
        else:
            print(f"Specific record retrieved successfully: {response.json()}")

    @task
    def update_record(self):
        """Simulate PUT request to update a record in the database."""
        if not DBUser.update_executed:
            params = {
                "update_values": '{"vehicle_make": "Toyota", "vehicle_model": "Camry"}',
                "key": "82HFE9767U326DEZ2",
            }
            response = self.client.put("/db/update/", params=params)
            if response.status_code != 200:
                print(f"Error: {response.status_code}, {response.text}")
            else:
                print(f"Record updated successfully: {response.json()}")
            DBUser.update_executed = True  # Set the flag to prevent re-execution

    @task
    def delete_record(self):
        """Simulate DELETE request to delete a specific record from the database."""
        if not DBUser.delete_executed:
            params = {"key": "H1AUMH0D9M76R7NNG"}
            response = self.client.delete("/db/delete/", params=params)
            if response.status_code != 200:
                print(f"Error: {response.status_code}, {response.text}")
            else:
                print(f"Record deleted successfully: {response.json()}")
            DBUser.delete_executed = True  # Set the flag to prevent re-execution
