# Imports
import psycopg2
import pandas as pd

import sys
sys.path.append('../scripts')
import functions as f

class ImmoDB():
    """ This class aims to facilitate connections to ImmoScrap platform (PostGres for start)
    """

    def __init__(self, config):
        
        # Connection to Postgre
        self.config = config
        self.get_params(config)
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.pwd)
        self.cur = self.conn.cursor()


    #####################################################################################
    #                              BASICS
    #####################################################################################

    
    def get_params(self, config):
        self.host = 'localhost'
        self.db = 'immoscrap'
        self.user = 'root'
        self.pwd = 'password'


    def execute_sql(self, sql_request):
        """ Execute an sql request on the specified dev environment
        """
        self.cur.execute(sql_request)
        # response = self.cur.fetchall()
        return self.cur


    def reset_connection(self):
        """In case of problem in a request, there might be a connection issue.
            This function reset the connection and allows to keep the object alive.
        """
        self.conn.close()
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.pwd
            )
        self.cur = self.conn.cursor()


    #####################################################################################
    #                              SOPHISTICATED
    #####################################################################################


    def sql_to_df(self, sql_request, with_col=False, index=None):
        """Converts an SQL request into a dataFrame
        - with_col : If True, send an other requests to get the column names of the table and set it to the DataFrame
        - index : if specified, set the selected column as index of the DataFrame 
        """
        self.cur.execute(sql_request)
        rows = self.cur.fetchall()
        df = pd.DataFrame(rows)

        if with_col:
            try:
                req = sql_request.split('FROM')[1]
            except:
                req = sql_request.split('from')[1]

            table_name = req.split(' ')[1]
            print('Table name :', table_name)
            columns = self.get_column_names(table_name)
            df.columns = columns
            if index is not None:
                df = df.set_index(index)
        return df



    def get_df_with_id_as_index(self, sql_request):
        """Use the sql_to_df functions with specific parameters (Get column names = True, index=id)
        - Warning : will not work with materialized view (only table)
        """
        df = self.sql_to_df(sql_request, with_col=True, index='id')
        return df


    def get_column_names(self, table_name):
        """ This function gets the column names of a table reqeusting its schema
        - Warning : will not work with materialized view (only classical table)
        """
        sql = """select column_name
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='{}';""".format(table_name)
        self.cur.execute(sql)
        columns = self.cur.fetchall()
        columns = [i[0] for i in columns]
        return columns


if __name__ == "__main__":

    # Getting config
    CONFIG_PATH = '../config/config.json'
    config = f.read_json(CONFIG_PATH)
    db_config = config['database']

    # Starting object
    immodb=ImmoDB(db_config)

    # Making a test
    df = immodb.sql_to_df('SELECT * FROM raw_scrapped')
    
    # Displaying result
    print(df.head())
  








  # def create_sql_request_header(dataframe, table_name):
#     """ (1/2) Used in the process of inserting data into a pocoto database.
#         This is the creation of the header of the request
#         Part 2 is 'create_sql_request_row' function
#     """
#     s = "INSERT INTO "
#     s += table_name + "("
#     for col in dataframe.columns:
#         s+=col.lower() + ","
#     s = s[:-1] + ") VALUES ("
#     for i in range(len(dataframe.columns)):
#         s+= "%s,"
#     s = s[:-1] + ");"
#     return s

# def create_sql_request_row(dataframe_row):
#     """ (2/2) Used in the process of inserting data into a pocoto database.
#         This is the creation of the content of the request
#         Part 1 is 'create_sql_request_header' function
#     """
#     y = []
#     for i in range(len(dataframe_row)):
#         if pd.isnull(dataframe_row[i]) or dataframe_row[i] == "":
#             y.append(None)
#         else:
#             val = dataframe_row[i]
#             y.append(val)
#     return y

# def execute_sql_insert(pocoto, dataframe,table_name):
#     """ Used to insert data into a pocoto DataBase, given a DataFrame and a table_name.
#         NB : The table must have the columns of the DataFrame as fields.
#     """
#     cur = pocoto.cur
#     sql_request = create_sql_request_header(dataframe, table_name)
#     print(sql_request)
#     for index,rows in dataframe.iterrows():
#         my_sql_row = create_sql_request_row(rows)
#         print(my_sql_row)
#         cur.execute(sql_request,my_sql_row)
#     pocoto.conn.commit()
#     print('> {} : dataframe correctly inserted.'.format(table_name))