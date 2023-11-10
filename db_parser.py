import json
import pymysql
from config import settings


def db_update(file, table):
    json_obj = json.loads(file)
    con = pymysql.connect(host=settings.host, user=settings.user, password=settings.password, db=settings.database)
    cursor = con.cursor()

    if table == "user":
        for item in json_obj:
            id = item.get("id")
            first_name = item.get("first_name")
            last_name = item.get("last_name")
            nickname = item.get("user")
            phone = item.get("phone")
            cursor.execute(f"INSERT INTO parser.{table}"
                           "(id, first_name, last_name, nickname, phone) VALUES (%s,%s,%s,%s,%s)"
                           "ON DUPLICATE KEY UPDATE id=%s, first_name=%s, last_name=%s, nickname=%s, phone=%s",
                           (id, first_name, last_name, nickname, phone,
                            id, first_name, last_name, nickname, phone))
        con.commit()
        con.close()
    else:
        for item in json_obj:
            try:

                id = item.get("id")
                link = item.get("link")
                timedate = item.get("date")
                timedate = timedate.replace("T", " ")[:-6]
                deleted = item.get("deleted")
                message = item.get("message")
                from_id = item.get("from_id")

                media = item.get("media")
                if media is None:
                    media = '-'
                else:
                    media = ", ".join(media)
                cursor.execute(
                    f"INSERT INTO parser.{table}"
                    "(id, link, timedate, deleted, message, from_id, media) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    "ON DUPLICATE KEY UPDATE deleted=%s, message=%s, media=%s",
                    (id, link, timedate, deleted, message, from_id, media,
                     deleted, message, media))

            except:
                continue

        con.commit()
        con.close()



json_users = open("files/channel_users.json", encoding="utf8").read()
json_messages = open("files/channel_messages.json", encoding="utf8").read()
json_sale_messages = open("files/sale_messages.json", encoding="utf8").read()
json_buy_messages = open("files/buy_messages.json", encoding="utf8").read()
json_other_messages = open("files/other_messages.json", encoding="utf8").read()


db_update(json_users, "user")
db_update(json_messages, "message")
db_update(json_sale_messages, "sale")
db_update(json_buy_messages, "buy")
db_update(json_other_messages, "other")

