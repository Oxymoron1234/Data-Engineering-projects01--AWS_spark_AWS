import mysql.connector
from resources.dev.config import  *



def get_mysql_connection():
    connection = mysql.connector.connect(
        host=host_name,
        user= properties['user'],
        password=properties['password'],
        database=database_name
    )
    return connection

















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="demo"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM demo.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()

####################trying to create tables inside selected database

# from mysql.connector import errorcode
#
# try:
#     connection = mysql.connector.connect(
#         host=host_name,
#         user= properties['user'],
#         password=properties['password'],
#         database=database_name
#     )
#     print("Connection successful!")
#
#
#     def create_care_table(connection ,statement):
#         cursor = connection.cursor()
#         create_table_query = statement
#         cursor.execute(create_table_query)
#         connection.commit()
#         print("Care table created successfully!")
#         cursor.close()
#     s = []
#     s.append("""CREATE TABLE  IF NOT EXIST product_staging_table(
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     file_name VARCHAR(255),
#     file_location VARCHAR(255),
#     created_date TIMESTAMP ,
#     updated_date TIMESTAMP ,
#     status VARCHAR(1)
#     );""")
#
#     s.append("""CREATE TABLE  IF NOT EXIST customer(
#     customer_id INT AUTO_INCREMENT PRIMARY KEY,
#     first_name VARCHAR(50),
#     last_name VARCHAR(50),
#     address VARCHAR(255),
#     pincode VARCHAR(10),
#     phone_number VARCHAR(20),
#     customer_joining_date DATE
# );""")
#
#     s.append("""CREATE TABLE  IF NOT EXIST store(
#     id INT PRIMARY KEY,
#     address VARCHAR(255),
#     store_pincode VARCHAR(10),
#     store_manager_name VARCHAR(100),
#     store_opening_date DATE,
#     reviews TEXT
# );
# """)
#
#     s.append("""CREATE TABLE  IF NOT EXIST product(
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     name VARCHAR(255),
#     current_price DECIMAL(10, 2),
#     old_price DECIMAL(10, 2),
#     created_date TIMESTAMP ,
#     updated_date TIMESTAMP ,
#     expiry_date DATE
# );""")
#
#     s.append("""CREATE TABLE  IF NOT EXIST sales_team(
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     first_name VARCHAR(50),
#     last_name VARCHAR(50),
#     manager_id INT,
#     is_manager CHAR(1),
#     address VARCHAR(255),
#     pincode VARCHAR(10),
#     joining_date DATE
# );""")
#
#     s.append("""CREATE TABLE  IF NOT EXIST s3_bucket_info(
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     bucket_name VARCHAR(255),
#     file_location VARCHAR(255),
#     created_date TIMESTAMP ,
#     updated_date TIMESTAMP ,
#     status VARCHAR(20)
# );
# """)
#     s.append("""CREATE TABLE  IF NOT EXIST customers_data_mart(
#     customer_id INT ,
#     full_name VARCHAR(100),
#     address VARCHAR(200),
#     phone_number VARCHAR(20),
#     sales_date_month DATE,
#     total_sales DECIMAL(10, 2)
# );""")
#     s.append("""CREATE TABLE  IF NOT EXIST sales_team_data_mart(
#     store_id INT,
#     sales_person_id INT,
#     full_name VARCHAR(255),
#     sales_month VARCHAR(10),
#     total_sales DECIMAL(10, 2),
#     incentive DECIMAL(10, 2)
# );""")

    # for i in s:
    #     create_care_table(connection, i)


    # def insert_care_record(connection, patient_name, care_type, care_date, notes):
    #     cursor = connection.cursor()
    #     insert_query = """
    #     INSERT INTO care (patient_name, care_type, care_date, notes)
    #     VALUES (%s, %s, %s, %s);
    #     """
    #     cursor.execute(insert_query, (patient_name, care_type, care_date, notes))
    #     connection.commit()
    #     print("Record inserted successfully!")
    #      cursor.close()


    # insert_care_record(connection, 'John Doe', 'Physical Therapy', '2024-10-01', 'Patient showed improvement.')
    #
    #
    # def fetch_care_records(connection):
    #     cursor = connection.cursor()
    #     select_query = "SELECT * FROM care;"
    #     cursor.execute(select_query)
    #
    #     records = cursor.fetchall()
    #     for record in records:
    #         print(record)
    #
    #     cursor.close()
    #
    #
    # fetch_care_records(connection)

#     connection.close()
#     print("Connection closed.")
#
# except mysql.connector.Error as err:
#     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#         print("Something is wrong with your user name or password")
#     elif err.errno == errorcode.ER_BAD_DB_ERROR:
#         print("Database does not exist")
#     else:
#         print(err)

