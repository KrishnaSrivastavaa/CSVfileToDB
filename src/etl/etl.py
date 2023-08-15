import csv
import os
from src.etl.database.database import Database
from src.etl.logger.logger import logger

class ETL:
    def __init__(self):
        self.db = Database()
        self.conn = self.db.connect()

    def transform(self, row):
        first_name, last_name = row['First Name'], row['Last Name']
        row['Full Name'] = f"{first_name} {last_name}"
        return row

    def load(self, data):
        insert_query = "INSERT INTO employees (first_name, last_name, full_name, other_columns...) VALUES (%s, %s, %s, %s, ...)"
        for row in data:
            transformed_row = self.transform(row)
            values = (transformed_row['First Name'], transformed_row['Last Name'], transformed_row['Full Name'], ...)
            self.db.execute_query(self.conn, insert_query, values)
        logger.info("Data loaded successfully")

    def process(self):
        try:
            with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', '..', 'data', 'employees.csv'), 'r') as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]
                self.load(data)
        except Exception as e:
            logger.error("Error processing data: %s", e)
            raise
        finally:
            self.conn.close()

if __name__ == "__main__":
    etl = ETL()
    etl.process()
