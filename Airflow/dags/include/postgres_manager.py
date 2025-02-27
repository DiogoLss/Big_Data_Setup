import pyodbc

class Postgres_Connector:

    def __init__(self
                #  ,server,database
            ):
        # self.server = server
        # self.database = database
        self.conn = None

    def open_connection(self):
        #implementar lógica de qual server\banco trazer.
        conn_str = (
            "DRIVER={PostgreSQL Unicode};"
            "SERVER=postgres_simulation;"
            "PORT=5432;"
            "DATABASE=simulation;"
            "UID=postgres;"
            "PWD=postgres;"
        )
        self.conn = pyodbc.connect(conn_str)

    def get_cursor(self):
        return self.conn.cursor()
    
    def commit(self):
        self.conn.commit()

    def close_cursor(self,cursor):
        cursor.close()

    def close_connection(self):
        self.conn.close()