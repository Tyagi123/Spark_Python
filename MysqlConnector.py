import mysql.connector

def get_connection():
    try:
        config = {
            'user': 'root',
            'password': 'root',
            'host': '127.0.0.1',
            'database': 'TEST_DB',
            'raise_on_warnings': True,
            'use_pure': False,
        }
        return mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))



cnx = get_connection()
courser = cnx.cursor()
query = ("select * from quotes")
courser.execute(query)

for (id, name, quotes) in courser:
    print(name + "  " + quotes)

courser.close()
cnx.close()
