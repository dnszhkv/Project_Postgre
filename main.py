import psycopg2
from flask import Flask, request, jsonify
from decouple import config
import json
import base64

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

    # удаляю таблицы, если они существуют
    with conn.cursor() as cursor:
        cursor.execute(
            'DROP TABLE IF EXISTS "public"."pereval_added_images", "public"."pereval_added", '
            '"public"."pereval_images", "public"."coords", "public"."users" CASCADE;')

    # создаю таблицы
    with conn.cursor() as cursor:
        # таблица users
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."users" (
                    "id" serial PRIMARY KEY,
                    "email" text UNIQUE NOT NULL,
                    "phone" text,
                    "fam" text,
                    "name" text,
                    "otc" text
                );
            """)

        # таблица coords
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."coords" (
                    "id" serial PRIMARY KEY,
                    "latitude" double precision,
                    "longitude" double precision,
                    "height" integer
                );
            """)

        # таблица pereval_images
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."pereval_images" (
                    "id" serial PRIMARY KEY,
                    "date_added" timestamp,
                    "img" bytea,
                    "title" text
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
                    "coord_id" integer REFERENCES "public"."coords"("id"),
                    "level_winter" text,
                    "level_summer" text,
                    "level_autumn" text,
                    "level_spring" text,
                    "user_id" integer REFERENCES "public"."users"("id")
                );
            """)

        # таблица связи pereval_images и pereval_added
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS "public"."pereval_added_images" (
                    "pereval_added_id" integer REFERENCES "public"."pereval_added"("id"),
                    "image_id" integer REFERENCES "public"."pereval_images"("id"),
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

except Exception as ex:
    print('[INFO] Error while working with PostgresSQL', ex)
finally:
    if conn:
        conn.close()
        print('[INFO] PostgresSQL connection is closed')


# Метод submitData
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
                # добавляю пользователя
                user = data.get("user", {})
                email = user.get("email")
                phone = user.get("phone")
                fam = user.get("fam")
                name = user.get("name")
                otc = user.get("otc")
                cursor.execute(
                    """
                    INSERT INTO "public"."users" ("email", "phone", "fam", "name", "otc")
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ("email") DO NOTHING;
                    """,
                    (email, phone, fam, name, otc),
                )

                # добавляю координаты
                coords = data.get("coords", {})
                latitude = coords.get("latitude")
                longitude = coords.get("longitude")
                height = coords.get("height")
                cursor.execute(
                    """
                    INSERT INTO "public"."coords" ("latitude", "longitude", "height")
                    VALUES (%s, %s, %s)
                    RETURNING "id";
                    """,
                    (latitude, longitude, height),
                )
                coord_id = cursor.fetchone()[0]

                # добавляю уровень сложности
                level = data.get("level", {})
                winter = level.get("winter")
                summer = level.get("summer")
                autumn = level.get("autumn")
                spring = level.get("spring")

                # добавляю изображения
                images = data.get("images", [])
                image_ids = []
                for img_data in images:
                    img_binary = base64.b64decode(img_data["data"])
                    cursor.execute(
                        """
                        INSERT INTO "public"."pereval_images" ("date_added", "img", "title")
                        VALUES (CURRENT_TIMESTAMP, %s, %s)
                        RETURNING "id";
                        """,
                        (img_binary, img_data["title"]),
                    )
                    image_id = cursor.fetchone()[0]
                    image_ids.append(image_id)

                # добавляю перевал
                cursor.execute(
                    """
                    INSERT INTO "public"."pereval_added" (
                        "date_added", "beautyTitle", "title", "other_titles", "connect", "add_time",
                        "raw_data", "coord_id", "level_winter", "level_summer", "level_autumn", "level_spring", "user_id"
                    )
                    VALUES (
                        CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        (SELECT "id" FROM "public"."users" WHERE "email" = %s)
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

                # связываю изображения с перевалом
                for image_id in image_ids:
                    cursor.execute(
                        """
                        INSERT INTO "public"."pereval_added_images" ("pereval_added_id", "image_id")
                        VALUES (%s, %s);
                        """,
                        (pereval_id, image_id),
                    )

            return {"status": 200, "message": "Отправлено успешно", "id": pereval_id}

        except Exception as ex:
            print('[INFO] Error while submitting data to PostgreSQL', ex)
            return {"status": 500, "message": "Ошибка при выполнении операции", "id": None}

    # закрываю подключение
    def close_connection(self):
        if self.conn:
            self.conn.close()
            print('[INFO] PostgreSQL connection is closed')


# Создаю экземпляр класса PerevalDatabase
pereval_db = PerevalDatabase()


@app.route('/submitData', methods=['POST'])
def submit_data():
    try:
        # получаю данные из тела запроса
        data = request.get_json()

        # вызываю метод submit_data для обработки данных и добавления в базу данных
        result = pereval_db.submit_data(data)

        return jsonify(result)

    except Exception as ex:
        print('[INFO] Error while processing request', ex)
        return jsonify({"status": 500, "message": "Internal Server Error", "id": None})


if __name__ == '__main__':
    # считываю данные из json-файла
    with open('data.json', 'r', encoding='utf-8') as file:
        json_data = json.load(file)

    # вызываю метод submit_data для обработки данных и добавления их в базу данных
    result = pereval_db.submit_data(json_data)

    app.run(debug=True)
