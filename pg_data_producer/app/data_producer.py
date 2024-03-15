import os
from pathlib import Path
import string
from time import sleep
from psycopg2 import connect
import random
from random import randrange
from datetime import timedelta, date


class DataProducer:
    def __init__(self, database_configuration):
        self.connection = connect(**database_configuration)
        self.sleep_time: int = float(os.environ.get("SECONDS_BETWEEN_MODIFICATIONS", 10.0))

    def run_script(self, script_path: str, query_parameters: tuple | None = None):
        try:
            cursor = self.connection.cursor()
            sql_script_content = Path(script_path).read_text()
            cursor.execute(sql_script_content, query_parameters)
            self.connection.commit()
        except Exception as e:
            print(e)

    def generate_id(self, size=6, chars=string.ascii_uppercase + string.digits):
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

        # run initial ddl script
        self.run_script(script_path="./sql/ddl.sql")
        print("DDL script: ", ddl_script, "has been executed")

        print("Starting the process with an interval in seconds:", self.sleep_time)

        running = True
        while running:
            random_int = random.randint(0, 1000)
            random_date = self.generate_date(
                date.today() - timedelta(days=2000), date.today()
            )
            random_string = self.generate_id()

            merge_parameters = (random_int, random_string, random_date)

            self.run_script(script_path=merge_script, query_parameters=merge_parameters)
            print(f"--> {merge_parameters}")
            

            sleep(self.sleep_time)
