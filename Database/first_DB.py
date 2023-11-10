from getpass import getpass
from mysql.connector import connect, Error
from config import settings

# online_movie_rating
try:
    with (connect(
            host=settings.host,
            user=settings.user,
            password=settings.password,
            db=settings.database) as connection):
        print(connection)
except Error as e:
    print(e)

create_movies_table_query = """
CREATE TABLE movies(
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(100),
    release_year YEAR(4),
    genre VARCHAR(100),
    collection_in_mil INT
)
"""

with connection.cursor() as cursor:
    cursor.execute(create_movies_table_query)
    connection.commit()

create_reviewers_table_query = """
CREATE TABLE reviewers(
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100
)
"""

with connection.cursor() as cursor:
    cursor.execute(create_reviewers_table_query)
    connection.commit()

create_ratings_table_query = """
CREATE TABLE ratings(
    movie_id INT
    reviewer_id INT,
    rating DECIMAL(2,1),
    FOREIGN KEY(movie_id) REFERENCES movies(id),
    FOREIGN KEY(reviewer_id) REFERENCES reviewers(id),
    PRIMARY KEY(movie_id, reviewer_id)
)
"""

with connection.cursor() as cursor:
    cursor.execute(create_ratings_table_query)
    connection.commit()

show_table_query = "DESCRIBE movies"
with connection.cursor() as cursor:
    cursor.execute(show_table_query)
    # Fetch rows from last executed query
    result = cursor.fetchall()
    for row in result:
        print(row)

"""def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB succsecful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

create_database_query = "CREATE DATABASE sm_app;"
connection = create_connection("localhost", "root", "elsi1979", "sm_app")

create_database(connection, create_database_query)"""
