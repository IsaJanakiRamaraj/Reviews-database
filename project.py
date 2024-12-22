import pandas as pd
import psycopg2

class DB:
    def postgress_conn():
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="pgadmin",
            host="127.0.0.1",
            port="5432"
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # Terminate other sessions
        terminate_query = """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'reviews' AND pid <> pg_backend_pid();
        """
        cur.execute(terminate_query)
        conn.commit()

        # Drop the database
        cur.execute("DROP DATABASE IF EXISTS reviews;")
        cur.execute("CREATE DATABASE reviews;")

        cur.close()
        conn.close()

        conn = psycopg2.connect(
            dbname="reviews",
            user="postgres",
            password="pgadmin",
            host="127.0.0.1",
            port="5432"
        )
        cur = conn.cursor()
        return conn, cur

walmart_csv = pd.read_csv("python_postgress/archive/ShoppingAppReviews Dataset/ShoppingAppReviews/csv/Walmart.csv")

# Checking for null values
# for key in walmart_csv.columns:
#     print(walmart_csv[key].isnull().sum())

# Replacing the null values
replace_values = {
    'replyContent': "Not Replied",
    'repliedAt': 0,
}
walmart_csv.fillna(value=replace_values, inplace=True)

# Deleting if any duplicates are there
walmart_csv.drop_duplicates(subset='reviewId', inplace=True)
print(walmart_csv)

# Adding a new column
walmart_csv['company'] = 'Walmart'

# Converting data to required datatype
walmart_csv['reviewId'] = walmart_csv['reviewId'].astype(str)
walmart_csv['content'] = walmart_csv['content'].astype(str)
walmart_csv['score'] = walmart_csv['score'].astype(int)
walmart_csv['thumbsUpCount'] = walmart_csv['thumbsUpCount'].astype(int)
walmart_csv['at'] = walmart_csv['at'].astype(int)
walmart_csv['replyContent'] = walmart_csv['replyContent'].astype(str)
walmart_csv['repliedAt'] = walmart_csv['repliedAt'].astype(int)
walmart_csv['appName'] = walmart_csv['appName'].astype(str)
walmart_csv['company'] = walmart_csv['company'].astype(str)

walmart_csv.to_csv("C://Users//gopig//Desktop//project//python_postgress//output//walmart_cleaned_data.csv", index=False)

DB_obj = DB
conn, cur = DB_obj.postgress_conn()
create_review_table = """
CREATE TABLE IF NOT EXISTS review (
    reviewId VARCHAR(36),
    content TEXT,
    score INTEGER,
    thumbsUpCount INTEGER,
    at BIGINT,
    replyContent TEXT,
    repliedAt VARCHAR(36),
    appName VARCHAR(30),
    company VARCHAR(50)
)
"""
cur.execute(create_review_table)
conn.commit()

insert_data_into_review_table = """
INSERT INTO review (reviewId, content, score, thumbsUpCount, at, replyContent, repliedAt, appName, company)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for i, row in walmart_csv.iterrows():
    cur.execute(insert_data_into_review_table, list(row))
conn.commit()

cur.close()
conn.close()
