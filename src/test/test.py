# key = 's3://de-sales-project-01/sales_data/Drug_clean.csv'
# x = "de-sales-project-01"
#
# # Find the starting index of the keyword
# def get_object_key(text, keyword):
#
#     start_index = text.find(keyword)
#     after_keyword = ""
#     before_keyword = ""
#     if start_index != -1:
#         # Slice from the start to the keyword
#         before_keyword = text[:start_index]
#         # Slice from the keyword to the end
#         after_keyword = text[start_index + len(keyword) + 1:]
#
#         print("Before Keyword:", before_keyword.strip())  # Output: The quick brown
#         print("After Keyword:", after_keyword.strip())    # Output: jumps over the lazy dog.
#     else:
#         print("Keyword not found.")
#     return  after_keyword.strip()
#
# print(get_object_key(key , "/"))
from resources.dev.config import mandatory_columns, mandatory_columns_type, host_name, properties, database_name
import mysql.connector
from mysql.connector import Error

index = 0
x = ""
while index < len(mandatory_columns):
   x = x + mandatory_columns[index] + " " + mandatory_columns_type[index] + ", "
   index = index + 1
#x =x[:(len(x)-2):]
x = x + "additional_column varchar(1000)"



def create_connection():
    """Create a database connection to a MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=properties['user'],
            password=properties['password'],
            database=database_name
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def create_table(connection):
    """Create a table in the MySQL database."""
    try:
        cursor = connection.cursor()
        s = []
        s.append(f"""INSERT INTO store (id, address, store_pincode, store_manager_name, store_opening_date, reviews)
VALUES
    (121,'Delhi', '122009', 'Manish', '2022-01-15', 'Great store with a friendly staff.'),
    (122,'Delhi', '110011', 'Nikita', '2021-08-10', 'Excellent selection of products.'),
    (123,'Delhi', '201301', 'vikash', '2023-01-20', 'Clean and organized store.'),
    (124,'Delhi', '400001', 'Rakesh', '2020-05-05', 'Good prices and helpful staff.');
""")
        s.append(f"""INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date)
VALUES
    ('quaker oats', 212, 212, '2022-05-15', NULL, '2025-01-01'),
    ('sugar', 50, 50, '2021-08-10', NULL, '2025-01-01'),
    ('maida', 20, 20, '2023-03-20', NULL, '2025-01-01'),
    ('besan', 52, 52, '2020-05-05', NULL, '2025-01-01'),
    ('refined oil', 110, 110, '2022-01-15', NULL, '2025-01-01'),
    ('clinic plus', 1.5, 1.5, '2021-09-25', NULL, '2025-01-01'),
    ('dantkanti', 100, 100, '2023-07-10', NULL, '2025-01-01'),
    ('nutrella', 40, 40, '2020-11-30', NULL, '2025-01-01');""")
        s.append(f"""INSERT INTO sales_team (first_name, last_name, manager_id, is_manager, address, pincode, joining_date)
VALUES
    ('Rahul', 'Verma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Priya', 'Singh', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Amit', 'Sharma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Sneha', 'Gupta', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Neha', 'Kumar', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Vijay', 'Yadav', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Anita', 'Malhotra', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Alok', 'Rajput', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Monica', 'Jain', 10, 'N', 'Delhi', '122007', '2020-05-01'),
    ('Rajesh', 'Gupta', 10, 'Y', 'Delhi', '122007', '2020-05-01');""")
        s.append(f"""INSERT INTO s3_bucket_info (bucket_name, status, created_date, updated_date)
VALUES ('{database_name}', 'active', NOW(), NOW());""")
        # print(f"query is {create_table_query}")
        s.append(f"""INSERT INTO customer (customer_id, first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES
         (1,'Saanvi', 'Krishna', 'Delhi', '122009', '9173121081', '2021-01-20'),
         (2,'Dhanush', 'Sahni', 'Delhi', '122009', '9155328165', '2022-03-27'),
         (3,'Yasmin', 'Shan', 'Delhi', '122009', '9191478300', '2023-04-08'),
         (4,'Vidur', 'Mammen', 'Delhi', '122009', '9119017511', '2020-10-12'),
         (5,'Shamik', 'Doctor', 'Delhi', '122009', '9105180499', '2022-10-30'),
         (6,'Ryan', 'Dugar', 'Delhi', '122009', '9142616565', '2020-08-10'),
         (7,'Romil', 'Shanker', 'Delhi', '122009', '9129451313', '2021-10-29'),
         (9,'Krish', 'Tandon', 'Delhi', '122009', '9145683399', '2020-01-08'),
         (10,'Divij', 'Garde', 'Delhi', '122009', '9141984713', '2020-11-10'),
         (11,'Hunar', 'Tank', 'Delhi', '122009', '9169808085', '2023-01-27'),
         (13,'Zara', 'Dhaliwal', 'Delhi', '122009', '9129776379', '2023-06-13'),
         (14,'Sumer', 'Mangal', 'Delhi', '122009', '9138607933', '2020-05-01'),
         (15,'Rhea', 'Chander', 'Delhi', '122009', '9103434731', '2023-08-09'),
         (16,'Yuvaan', 'Bawa', 'Delhi', '122009', '9162077019', '2023-02-18'),
         (17,'Sahil', 'Sabharwal', 'Delhi', '122009', '9174928780', '2021-03-16'),
         (18,'Tiya', 'Kashyap', 'Delhi', '122009', '9105126094', '2023-03-23'),
         (19,'Kimaya', 'Lala', 'Delhi', '122009', '9115616831', '2021-03-14'),
         (21,'Vardaniya', 'Jani', 'Delhi', '122009', '9125068977', '2022-07-19'),
         (22,'Indranil', 'Dutta', 'Delhi', '122009', '9120667755', '2023-07-18'),
         (23,'Kavya', 'Sachar', 'Delhi', '122009', '9157628717', '2022-05-04'),
         (25,'Manjari', 'Sule', 'Delhi', '122009', '9112525501', '2023-02-12'),
         (26,'Akarsh', 'Kalla', 'Delhi', '122009', '9113226332', '2021-03-05'),
         (27,'Miraya', 'Soman', 'Delhi', '122009', '9111455455', '2023-07-06'),
         (29,'Shalv', 'Chaudhary', 'Delhi', '122009', '9158099495', '2021-03-14'),
         (30,'Jhanvi', 'Bava', 'Delhi', '122009', '9110074097', '2022-07-14');
""")
        for i in s:
            cursor.execute(i)

            connection.commit()
            print(f"data inserted sucessfully {i}")
        # print("Table created successfully")
    except Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

def main():
    connection = create_connection()
    if connection:
        create_table(connection)
        connection.close()

if __name__ == "__main__":
    main()



