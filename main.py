import psycopg2
from config import *


try:
    # создаю подключение к базе данных
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=dbname,
    )

    conn.autocommit = True

    # создаю таблицы
    with conn.cursor() as cursor:
        cursor.execute(
            ''
        )


except Exception as _ex:
    print('[INFO] Error while working with PostgresSQL', _ex)
finally:
    if conn:
        conn.close()
        print('[INFO] PostgresSQL connection is closed')
