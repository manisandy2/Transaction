import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error
from .db_columns import transaction_columns
# import mysql

load_dotenv()

def mysql_connect():
    print("Connecting to MySQL database...")

    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=3306
        )
        if conn.is_connected():
            print("✅ MySQL connection established")
            return conn
        else:
            raise ConnectionError("❌ MySQL connection could not be established")
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


class MysqlCatalog:
    def __init__(self) -> None:
        self.conn = mysql_connect()
        if self.conn is None:
            raise RuntimeError("Database connection failed")
        self.cursor = self.conn.cursor(dictionary=True)

        # self.table_name = "employees"
        # self.table_name = "Transaction"



    def get_all_value(self,table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_count(self,table_name:str):
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")

        return self.cursor.fetchone()["COUNT(*)"]

    def get_describe(self,table_name:str):
        self.cursor.execute(f"DESCRIBE {table_name}")
        # print("Describe:",self.cursor.fetchall())
        # return self.cursor.fetchall()
        result = self.cursor.fetchall()
        # print("Describe:", result)
        return result

    def get_range(self, table_name: str, start: int, end: int):
        try:
            columns = ", ".join(transaction_columns)
            # self.cursor.execute(f"USE {dbname};")
            query = f"""
                SELECT
                    {columns}
                FROM {table_name}
                ORDER BY pri_id ASC
                LIMIT %s, %s
            """
            limit = end - start
            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in get_range_ph_bi: {e}")
            return []

    def get_date_range(self, dbname, start_date, end_date, offset, limit):
        query = f"""
            SELECT *
            FROM `{dbname}`
            WHERE created_At BETWEEN %s AND %s
            ORDER BY created_At ASC
            LIMIT %s OFFSET %s
        """
        self.cursor.execute(query, (start_date, end_date, limit, offset))
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ss = MysqlCatalog()
# print(ss.get_range(table_name="Transaction",start=1,end=2))