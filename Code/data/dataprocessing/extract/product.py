<<<<<<< HEAD
from pyspark.sql.functions import date_format, to_date, col, lit, when ,to_timestamp
=======
from pyspark.sql.functions import date_format
>>>>>>> c5fe626bf90a975b2e1e7a7a7d90b5e5ef67370e

def product_transformation(df=None, column_list=None):
    """
    Transform the input DataFrame by filtering rows based on 'status' and 'status_for_sale' values,
    selecting specific columns, and formatting date columns.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing the data.
    - column_list (list): List of columns to select from the DataFrame.

    Returns:
    pandas.DataFrame: Transformed DataFrame with filtered rows, selected columns, and formatted date columns.

    Note:
    - The function filters out rows where 'status' or 'status_for_sale' is equal to 2.
    - Date columns 'published_at', 'created_at', and 'updated_at' are formatted to 'yyyy-MM-dd HH:mm:ss'.
    - The selected columns include 'product_id', 'product_title', 'published_at', 'product_thumbnail', 'created_at', and 'updated_at'.
    """
    # Filter rows based on status and status_for_sale

    print(df.status)
    df = df.filter((df.status != 2) | (df.status_for_sale != 2))

    # Select the specified columns
<<<<<<< HEAD
    
    # try :
    #     df = df.withColumn("published_at", 
    #                when(
    #                    (to_date(col("published_at"), 'yyyy-MM-dd HH:mm:ss').isNotNull()) & 
    #                    (col("published_at") != "0000-00-00 00:00:00"),
    #                    date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss")
    #            ).otherwise(None))

    
    # except:
    #     print("not working for bango_live")
    try :
        df = df.withColumn("published_at", 
                       when(
                    (to_timestamp(col("published_at"), 'yyyy-MM-dd HH:mm:ss').isNotNull()),
                    date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss")
                   )
                   .otherwise(None))
   
        df = df.select(column_list)

    except:
        print("not working for development_db")



    # Convert to Pandas DataFrame
    df = df.toPandas()


=======
    # columns = ['product_id', 'product_title', 'published_at', 'product_thumbnail', 'created_at', 'updated_at']
    print(column_list)
    try:
        df = df.select(column_list)
    except :
        print('column not found')
    # Format date columns
    df = df.withColumn("published_at", date_format("published_at", "yyyy-MM-dd HH:mm:ss"))
    # df = df.withColumn("created_at", date_format("created_at", "yyyy-MM-dd HH:mm:ss"))
    # df = df.withColumn("updated_at", date_format("updated_at", "yyyy-MM-dd HH:mm:ss"))

    # Convert the DataFrame to a Pandas DataFrame
    df = df.toPandas()
>>>>>>> c5fe626bf90a975b2e1e7a7a7d90b5e5ef67370e
    return df
