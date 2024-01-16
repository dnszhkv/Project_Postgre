import psycopg2
from flask import Flask, request, jsonify
from decouple import config
import json

# Получаем данные для подключения к базе данных из переменных окружения
host = config('FSTR_DB_HOST')
port = config('FSTR_DB_PORT')
user = config('FSTR_DB_LOGIN')
password = config('FSTR_DB_PASS')
dbname = config('FSTR_DB_NAME')

# Создание базы данных
try:
    # создаю подключение к базе данных
    conn = psycopg2.connect(
        host=host,
        port=port,
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


# Метод SubmitData
app = Flask(__name__)


class PerevalDatabase:
    def __init__(self):
        try:
            # создаю подключение к базе данных
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=dbname,
            )

            self.conn.autocommit = True

        except Exception as ex:
            print('[INFO] Error while working with PostgreSQL', ex)

    def submit_data(self, data):
        try:
            # создаю курсор для выполнения запросов
            with self.conn.cursor() as cursor:
                # добавление пользователя
                user = data.get("user", {})
                email = user.get("email")
                phone = user.get("phone")
                fam = user.get("fam")
                name = user.get("name")
                otc = user.get("otc")
                cursor.execute(
                    """
                    INSERT INTO "public"."Users" ("email", "phone", "fam", "name", "otc")
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ("email") DO NOTHING;
                    """,
                    (email, phone, fam, name, otc),
                )

                # добавление координат
                coords = data.get("coords", {})
                latitude = coords.get("latitude")
                longitude = coords.get("longitude")
                height = coords.get("height")
                cursor.execute(
                    """
                    INSERT INTO "public"."Coords" ("latitude", "longitude", "height")
                    VALUES (%s, %s, %s)
                    RETURNING "id";
                    """,
                    (latitude, longitude, height),
                )
                coord_id = cursor.fetchone()[0]

                # добавление уровня сложности
                level = data.get("level", {})
                winter = level.get("winter")
                summer = level.get("summer")
                autumn = level.get("autumn")
                spring = level.get("spring")

                # добавление изображений
                images = data.get("images", [])
                image_ids = []
                for img_data in images:
                    cursor.execute(
                        """
                        INSERT INTO "public"."PerevalImages" ("date_added", "img")
                        VALUES (CURRENT_TIMESTAMP, %s)
                        RETURNING "id";
                        """,
                        (img_data["data"],),
                    )
                    image_id = cursor.fetchone()[0]
                    image_ids.append(image_id)

                # добавление перевала
                cursor.execute(
                    """
                    INSERT INTO "public"."pereval_added" (
                        "date_added", "beautyTitle", "title", "other_titles", "connect", "add_time",
                        "raw_data", "coord_id", "level_winter", "level_summer", "level_autumn", "level_spring", "user_id"
                    )
                    VALUES (
                        CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        (SELECT "id" FROM "public"."Users" WHERE "email" = %s)
                    )
                    RETURNING "id";
                    """,
                    (
                        data.get("beauty_title"),
                        data.get("title"),
                        data.get("other_titles"),
                        data.get("connect"),
                        data.get("add_time"),
                        data.get("raw_data"),
                        coord_id,
                        winter,
                        summer,
                        autumn,
                        spring,
                        email,
                    ),
                )
                pereval_id = cursor.fetchone()[0]

                # связывание изображений с перевалом
                for image_id in image_ids:
                    cursor.execute(
                        """
                        INSERT INTO "public"."PerevalAddedImages" ("pereval_added_id", "image_id")
                        VALUES (%s, %s);
                        """,
                        (pereval_id, image_id),
                    )

            return {"status": 200, "message": "Отправлено успешно", "id": pereval_id}

        except Exception as ex:
            print('[INFO] Error while submitting data to PostgreSQL', ex)
            return {"status": 500, "message": "Ошибка при выполнении операции", "id": None}

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print('[INFO] PostgreSQL connection is closed')


# Создаю экземпляр класса PerevalDatabase
pereval_db = PerevalDatabase()


@app.route('/submitData', methods=['POST'])
def submit_data():
    try:
        # Получаю данные из тела запроса
        data = request.get_json()

        # Вызываю метод submit_data для обработки данных и добавления в базу данных
        result = pereval_db.submit_data(data)

        return jsonify(result)

    except Exception as ex:
        print('[INFO] Error while processing request', ex)
        return jsonify({"status": 500, "message": "Internal Server Error", "id": None})


if __name__ == '__main__':
    # Считываем данные из файла data.json
    with open('data.json', 'r', encoding='utf-8') as file:
        json_data = json.load(file)

    # Вызываем метод submit_data для обработки данных и добавления в базу данных
    result = pereval_db.submit_data(json_data)

    app.run(debug=True)
