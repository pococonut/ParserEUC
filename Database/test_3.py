import json
import pymysql

"""json_data=open("sample2.json").read()
json_obj=json.loads(json_data)
con=pymysql.connect(host="localhost", user="root", password=, db="user")

cursor = con.cursor()

for item in json_obj:
    firstname = item.get("firstName")
    lastname = item.get("lastName")
    gender = item.get("gender")
    age = item.get("age")
    cursor.execute("INSERT INTO user.new_table(firstname, lastname, gender, age) VALUE(%s,%s,%s,%s)", (firstname, lastname, gender, age))
con.commit()
con.close()"""

json_data=open(r"/TG_PARSER_Курсовая/channel_users.json", encoding="utf8").read()
json_obj=json.loads(json_data)
con=pymysql.connect(host="localhost", user="root", password='', db="channel_user")

cursor = con.cursor()

for item in json_obj:
    id = item.get("id")
    first_name = item.get("first_name")
    last_name = item.get("last_name")
    nickname = item.get("user")
    phone = item.get("phone")
    cursor.execute("INSERT INTO channel_user.test_1(id, first_name, last_name, nickname, phone) VALUE(%s,%s,%s,%s,%s)", (id, first_name, last_name, nickname, phone))
con.commit()
con.close()