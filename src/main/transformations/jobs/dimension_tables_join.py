from pyspark.sql.functions import *
from src.main.utility.logging_config import *
#enriching the data from different table
def dimesions_table_join(final_df_to_process,
                         customer_table_df,store_table_df,sales_team_table_df):

    #step 1 where i am adding customer table
    # final_df_to_process.alias("s3_data") \
    #     .join(customer_table_df.alias("ct"),
    #           col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
    #     .show()

    #But i do not need all the columns so dropping it
    #save the result into s3_customer_df_join
    logger.info("Joining the final_df_to_process with customer_table_df ")
    # s3_customer_df_join = final_df_to_process.alias("s3_data") \
    #     .join(customer_table_df.alias("ct"),
    #           col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
    #     .drop("product_name","price","quantity","additional_column",
    #           "s3_data.customer_id","customer_joining_date")
    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              final_df_to_process["customer_id"] == customer_table_df["customer_id"], "inner") \
        .drop("product_name", "price", "quantity", "additional_column",
              customer_table_df["customer_id"], "customer_joining_date")

    s3_customer_df_join.printSchema()
    s3_customer_df_join.show()

    #step 2 where i am adding store table details
    # s3_customer_df_join.join(store_table_df,
    #                          store_table_df["id"]==s3_customer_df_join["store_id"],
    #                          "inner").show()

    #But i do not need all the columns so dropping it
    #save the result into s3_customer_store_df_join
    logger.info("Joining the s3_customer_df_join (previous table result) with store_table_df ")
    s3_customer_store_df_join= s3_customer_df_join.join(store_table_df,
                             store_table_df["id"]==s3_customer_df_join["store_id"],
                             "inner")\
                        .drop("id","store_pincode","store_opening_date","reviews")
    s3_customer_store_df_join.show()

    #step 3 where i am adding sales team table details
    # s3_customer_store_df_join.join(sales_team_table_df,
    #                          sales_team_table_df["id"]==s3_customer_store_df_join["sales_person_id"],
    #                          "inner").show()


    #But i do not need all the columns so dropping it
    #save the result into s3_customer_store_sales_df_join
    logger.info("Joining the s3_customer_store_df_join (previous join result) with sales_team_table_df ")
    # s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
    #                          col("st.id")==s3_customer_store_df_join["sales_person_id"],
    #                          "inner")\
    #             .withColumn("sales_person_first_name",col("st.first_name"))\
    #             .withColumn("sales_person_last_name",col("st.last_name"))\
    #             .withColumn("sales_person_address",col("st.address"))\
    #             .withColumn("sales_person_pincode",col("st.pincode"))\
    #             .drop("id","st.first_name","st.last_name","st.address","st.pincode")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                                                                     sales_team_table_df["id"] == s3_customer_store_df_join["sales_person_id"],"inner") \
        .withColumn("sales_person_first_name", sales_team_table_df["first_name"]) \
        .withColumn("sales_person_last_name", sales_team_table_df["last_name"]) \
        .withColumn("sales_person_address", sales_team_table_df["address"]) \
        .withColumn("sales_person_pincode", sales_team_table_df["pincode"]) \
        .drop("id", sales_team_table_df["first_name"], sales_team_table_df["last_name"], sales_team_table_df["address"], sales_team_table_df["pincode"])

    return s3_customer_store_sales_df_join

