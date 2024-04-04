import os
from pathlib import Path
import string
from time import sleep
from typing import List
from psycopg2 import connect
import random
from random import randrange
from datetime import timedelta, date


class DataProducer:
    def __init__(self, database_configuration):
        self.connection = connect(**database_configuration)
        self.sleep_time: int = float(os.environ.get("SECONDS_BETWEEN_MODIFICATIONS", 10.0))

    def run_script(self, script_path: str, query_parameters: tuple | None = None) -> List[tuple] | None:
        """Run a SQL script and return the results

        Args:
            script_path (str): Path to the SQL script
            query_parameters (tuple | None, optional): Input parameters. Defaults to None.

        Returns:
            List[tuple] | None: Optional results of the query
        """
        
        results = None
        
        try:
            cursor = self.connection.cursor()
            sql_script_content = Path(script_path).read_text()
            cursor.execute(sql_script_content, query_parameters)
            if cursor.description:
                results = cursor.fetchall()
            self.connection.commit()           
            
            return results

        except Exception as e:
            print(e)

    def generate_id(self, size=6, chars=string.ascii_uppercase + string.digits):
        """Generate a random string

        Args:
            size (int, optional): Size of the generated string. Defaults to 6.
            chars (str, optional): The list of characters. Defaults to string.ascii_uppercase+string.digits.

        Returns:
            str: Generated string
        """
        return "".join(random.choice(chars) for _ in range(size))

    def generate_date(self, start, end):
        """
        This function will return a random datetime between two datetime
        objects.
        """
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def launch(self):
        ddl_script = "./sql/ddl.sql"
        merge_script = "./sql/merge.sql"
        delete_script = "./sql/delete.sql"

        # run initial ddl script
        self.run_script(script_path="./sql/ddl.sql")
        print("DDL script: ", ddl_script, "has been executed")

        print("Starting the process with an interval in seconds:", self.sleep_time)

        running = True
        while running:
            
            import random

            # every 5th time randomly, run the delete script
            operations = ["merge", "merge", "merge", "merge", "delete"]

            current_operation = random.choice(operations)
            
            if current_operation == "delete":
                results = self.run_script(script_path=delete_script)
                if results:
                    id = results[0][0]
                    print(f"--> delete ({id})")
                else:
                    print("No rows deleted")

                
            elif current_operation == "merge":                
                random_int = random.randint(1, 500000) # to generate 500k rows
                random_date = self.generate_date(
                    date.today() - timedelta(days=2000), date.today()
                )
                random_string = self.generate_id()

                merge_parameters = (random_int, random_string, random_date)

                self.run_script(script_path=merge_script, query_parameters=merge_parameters)
                print(f"--> merge  {merge_parameters}")
            else:
                print("Unknown operation")
            

            sleep(self.sleep_time)
