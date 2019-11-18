import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from datetime import datetime
import json

config = {
    'user': 'root',
    'password': 'asdf',
    'host': 'localhost',
    'database': 'fsport',
    'raise_on_warnings': True
}

def update_nba_slates_db(slate_data):

    salary = json.dumps(slate_data)
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        sql_select_query = """Update slates set salary = %s where site = %s and sport_type = %s"""
        cursor.execute(sql_select_query, (salary,'FanDuel', sport_type, ))
        connection.commit()
    except mysql.connector.Error as error:
        connection.rollback()
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        #closing database connection.
        if(connection.is_connected()):    
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def insert_nba_slates_into_db(slate_data, sport_type):

    salary = json.dumps(slate_data)
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        sql_select_query = """Update slates set salary = %s where site = %s and sport_type = %s"""
        cursor.execute(sql_select_query, (salary,'FanDuel', sport_type, ))
        connection.commit()
    except mysql.connector.Error as error:
        connection.rollback()
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        #closing database connection.
        if(connection.is_connected()):    
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def insert_slates_into_db(slate_data, sport_type):

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        sql_select_query = """select absid, slate_time, slate_name from slates where site = %s and sport_type = %s"""
        cursor.execute(sql_select_query, ('DraftKings', sport_type, ))
        records = cursor.fetchall()
        cursor.close()
        updated_slate = list()
        
        for row in records:
            index = row[2].find(')') + 1
            db_slate_name = '' if index == 0 else row[2][:index]
            existed = 0
            for slate in slate_data:
                if (db_slate_name == slate['title']):
                    existed = 1
                    break
            if (existed == 0 or slate_data == []):
                cursor = connection.cursor()
                sql_delete_query = """delete from slates where absid = %s"""
                cursor.execute(sql_delete_query, (row[0],))
                connection.commit()
                cursor.close()
            else:
                updated_slate.append({
                    'absid':row[0],
                    'slate_name':row[2]
                })
        for slate in slate_data:
            cursor = connection.cursor()
            slate_name = '' if slate['title'] == '' else slate['title'] + " " + str(slate['gameCount']) + " games"
            slate_time = slate['time']
            salary = json.dumps(slate['data'])

            data_salary = ('DraftKings', sport_type, slate_name, slate_time, salary)
            existed = 0
            for update in updated_slate:
              if (slate_name == update['slate_name']):
                  add_salary_query = """Update slates set salary = %s, slate_time = %s where site = %s and sport_type = %s and slate_name = %s"""
                  data_salary = (salary, slate_time, 'DraftKings', sport_type, slate_name)
                  cursor.execute(add_salary_query, data_salary)
                  connection.commit()
                  existed = 1
                  break
            if (existed == 0):
                add_salary_query = """INSERT INTO slates (site, sport_type, slate_name, slate_time, salary) VALUES (%s, %s, %s, %s, %s)"""
                cursor.execute(add_salary_query, data_salary)
                connection.commit()
            cursor.close()
    except mysql.connector.Error as error:
        connection.rollback()
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        #closing database connection.
        if(connection.is_connected()):    
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
def insert_projection_into_db(slate_data, sport_type, site):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # cur_date = "%" + datetime.today().strftime('%Y-%m-%d') + "%"
        if sport_type == "PGA":
            # slate_name = '' if slate['title'] == '' else slate['title'] + " " + str(slate['gameCount']) + " games"
            # slate_time = slate['time']
            update_projection_query = """Update slates set projection = %s where sport_type = %s and site = %s"""
            print(slate_data)
            data_projection = (json.dumps(slate_data), sport_type, site)
            cursor.execute(update_projection_query, data_projection)
            connection.commit()
        else:
            for key, value in slate_data.items():
                update_projection_query = """Update slates set projection = %s where sport_type = %s and site = %s ORDER BY slate_time DESC LIMIT 1"""
                data_projection = (json.dumps(value), sport_type, site)
                cursor.execute(update_projection_query, data_projection)
                connection.commit()
        
    except mysql.connector.Error as error:
        connection.rollback()
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        #closing database connection.
        if(connection.is_connected()):    
            cursor.close()
            connection.close()
            print("MySQL connection is closed")