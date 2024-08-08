from utils import connector
import mysql.connector as mysql

cnx,cur = connector(user='root', host='localhost')


def execute_sql_from_file(file_path, connection, cursor):
    """
    Executes SQL queries from a file on the given MySQL connection and cursor.

    Args:
        file_path (str): The path to the SQL file.
        connection (mysql.connector.connection.MySQLConnection): The MySQL connection object.
        cursor (mysql.connector.cursor.MySQLCursor): The MySQL cursor object.

    Raises:
        mysql.Error: If there is an error executing the SQL queries.
    """
    with open(file_path, 'r') as sql_file:
        sql = sql_file.read()

    try:
        for result in cursor.execute(sql, multi=True):
            if result.with_rows:
                print(f"Affected {result.rowcount} rows.")
        connection.commit()
        print("SQL queries executed successfully")
    except mysql.Error as err:
        print(f"Error: {err}")
        
execute_sql_from_file(file_path='src/join_tables.sql',
                      connection=cnx,
                      cursor=cur)