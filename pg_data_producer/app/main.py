from data_producer import DataProducer


def main():
    
    database_configuration = {
        "user": "postgres",
        "password": "postgres",
        "host": "db",
        "port": "5432",
        "database": "volok1",
    }
    
    dp = DataProducer(database_configuration=database_configuration)
    dp.launch()
        
if __name__ == '__main__':
    main()