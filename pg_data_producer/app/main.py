import os
from pathlib import Path
import string
from time import sleep
from psycopg2 import connect
import random
from random import randrange
from datetime import timedelta, date


conf = {
    "user": "postgres",
    "password":"postgres",
    "host": "db",
    "port": "5432",
    "database": "volok1"
} 


def get_connection():
    return connect(**conf)        

def run_script(script_path: str = "./ddl.sql"):
    try:
        connection = get_connection()
        cursor = connection.cursor()

        sql_script_content = Path(script_path).read_text()
        
        cursor.execute(sql_script_content)
        
        print("script executed: ", script_path)

        connection.commit()
    except Exception as e:
        print(e)

def insert_rows(id, name, rec_date):
    
    try:
        connection = get_connection()
        cursor = connection.cursor()

        postgres_insert_query = """
        
        MERGE INTO cdctable t
        USING (select %s as id, %s as name, cast(%s as date) as "date") s
        ON s.id = t.id
        WHEN MATCHED THEN
        UPDATE SET name = s.name, "date" = s."date"
        WHEN NOT MATCHED THEN
        INSERT (id, name, "date")
        VALUES (s.id, s.name, s."date");
        
        
        """
        values_to_insert = (id, name, rec_date)
        cursor.execute(postgres_insert_query, values_to_insert)

        connection.commit()
    except Exception as e:
        print(e)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def generate_random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def main():
    
    run_script()

    sleep_time: int = int(os.environ.get('SECONDS_BETWEEN_MODIFICATIONS', 10))
    
    print("Starting the process with an interval in seconds:", sleep_time)
    
    
    for i in range(10000):
        
        random_int = random.randint(0, 10000)
        random_date = generate_random_date(
            date.today() - timedelta(days=2000),
            date.today()
        )
        random_string = id_generator()
        
        values = {
            "id": random_int,
            "name": random_string,
            "rec_date": random_date,
        }
        
        insert_rows(**values)        
        print(f"--> {values}")
        
        sleep(sleep_time)
    


        
if __name__ == '__main__':
    main()