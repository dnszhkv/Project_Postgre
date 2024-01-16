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
        # таблица Users
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."Users" (
                    "id" serial PRIMARY KEY,
                    "email" text UNIQUE NOT NULL,
                    "phone" text,
                    "fam" text,
                    "name" text,
                    "otc" text
                );
            """)

        # таблица Coords
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."Coords" (
                    "id" serial PRIMARY KEY,
                    "latitude" double precision,
                    "longitude" double precision,
                    "height" integer
                );
            """)

        # таблица PerevalImages
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."PerevalImages" (
                    "id" serial PRIMARY KEY,
                    "date_added" timestamp,
                    "img" json
                );
            """)

        # таблица pereval_added
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."pereval_added" (
                    "id" serial PRIMARY KEY,
                    "date_added" timestamp,
                    "beautyTitle" text,
                    "title" text,
                    "other_titles" text,
                    "connect" text,
                    "add_time" timestamp,
                    "raw_data" json,
                    "coord_id" integer REFERENCES "public"."Coords"("id"),
                    "level_winter" text,
                    "level_summer" text,
                    "level_autumn" text,
                    "level_spring" text,
                    "user_id" integer REFERENCES "public"."Users"("id")
                );
            """)

        # таблица связи PerevalImages и pereval_added
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."PerevalAddedImages" (
                    "pereval_added_id" integer REFERENCES "public"."pereval_added"("id"),
                    "image_id" integer REFERENCES "public"."PerevalImages"("id"),
                    PRIMARY KEY ("pereval_added_id", "image_id")
                );
            """)

        # таблица pereval_areas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "public"."pereval_areas" (
                "id" serial PRIMARY KEY,
                "id_parent" integer,
                "title" text
            );
        """)

        # таблица spr_activities_types
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "public"."spr_activities_types" (
                "id" serial PRIMARY KEY,
                "title" text
            );
        """)

except Exception as _ex:
    print('[INFO] Error while working with PostgresSQL', _ex)
finally:
    if conn:
        conn.close()
        print('[INFO] PostgresSQL connection is closed')
