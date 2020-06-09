import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, when, concat, coalesce, regexp_replace, date_sub
from datetime import datetime
import mysql.connector

sc = SparkSession\
    .builder\
    .appName("mysqlconnect")\
    .getOrCreate()
sc.sparkContext.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

# Setting dates default. It will used in full join.
null_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
current_date = datetime.today().date()

dimension_conn = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'pass': '12345',
    'database': 'dw_adventure',
    'table': 'dim_contact'
}

staging_conn = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'pass': '12345',
    'database': 'st_adventure',
    'table': 'st_contact'
}


def change_dim_col_names(df_orig, type_operation=''):
    """
    This function put or remove 'dim_' prefix to columns. This is useful, because
    spark can't differentiate columns from staging and columns from dimension after the join.

    :param df_orig: input dataframe
    :param type_operation: 'add' or 'remove'
    :return: returns a treated dataframe
    """
    if type_operation == 'add':
        # rename columns. If i make a join with original column names, i can't choose the right one.
        for c, n in zip(df_orig.columns, ['dim_'+col for col in df_orig.columns]):
            df_orig = df_orig.withColumnRenamed(c, n)
    if type_operation == 'remove':
        for c, n in zip(df_orig.columns, [col.replace('dim_', '') for col in df_orig.columns]):
            df_orig = df_orig.withColumnRenamed(c, n)
    return df_orig


def concat_null_strings(*cols):
    """
    This function is used to concat first, middle and last name from a person. If a person
    haven't a middle name, for example, no space is added.

    :param cols: columns to make a treatment
    :return: name concatened
    """
    return concat(*[coalesce(c, lit('')) for c in cols])


def execute_action_db(host='localhost', user='root', passwd='12345', database='', sql='', val=[]):
    """
    Add many rows do mysql and handle with a connection.

    :param host: host
    :param user: user
    :param passwd: password
    :param database: database
    :param sql: sql command
    :param val: values to insert. Expect a list of list of values.
    """
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
        cursor = conn.cursor()

        cursor.executemany(sql, val)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)
    finally:
        cursor.close()
        conn.close()


# Read from staging
st_staging = sqlContext.read.format('jdbc').options(
        url='jdbc:mysql://{}:{}/{}'.format(staging_conn.get('host'), staging_conn.get('port'), staging_conn.get('database')),
        driver='com.mysql.jdbc.Driver',
        dbtable=staging_conn.get('table'),
        user=staging_conn.get('user'),
        password=staging_conn.get('pass')).load()


# Read from dimension
dim_contact = sqlContext.read.format('jdbc').options(
        url='jdbc:mysql://{}:{}/{}'.format(dimension_conn.get('host'), dimension_conn.get('port'), dimension_conn.get('database')),
        driver='com.mysql.jdbc.Driver',
        dbtable=dimension_conn.get('table'),
        user=dimension_conn.get('user'),
        password=dimension_conn.get('pass')).load().cache()

# Add prefix 'dim_' to dimension table.
dim_contact = change_dim_col_names(dim_contact, 'add')

# Add today date and null date in ends_at. Null date is 1900-01-01.
st_staging_treated = st_staging\
                        .withColumn('starts_at', lit(current_date))\
                        .withColumn('ends_at', lit(null_date))

# Concatenating name of person and put it in column.
st_staging_treated = st_staging_treated.withColumn(
    'name_concat',
    regexp_replace(
        concat_null_strings(
        'first_name', lit(' '),
        'middle_name', lit(' '),
        'last_name'), "  ", " ")
)

# Doing a full join to verify differences between staging and dimension tables. The join is based on code os client
# and date of ends_at.
staging_dw_df = dim_contact.join(
    st_staging_treated,
    (st_staging_treated.contact_id == dim_contact.dim_nk_cliente) & (st_staging_treated.ends_at == dim_contact.dim_ends_at),
    how='fullouter'
)

# Setting actions according to the value of column. In UPSERT all columns is verified (name_concat, email and telephone)
df_merge = staging_dw_df.withColumn(
    'action',
    when(((staging_dw_df.dim_nm_cliente != staging_dw_df.name_concat) |
          (staging_dw_df.dim_nm_email != staging_dw_df.email) |
          (staging_dw_df.dim_nr_phone != staging_dw_df.phone)), 'UPSERT')
    .when(staging_dw_df.contact_id.isNull() & staging_dw_df.dim_is_current == 1, 'DELETE')
    .when(staging_dw_df.dim_nk_cliente.isNull(), 'INSERT')
    .otherwise('NOACTION')
)

# Printing how many rows and its action.
print('********************************* How many rows and its actions *********************************')
df_merge.groupBy('action').count().show()

# Storing name of columns in dimension df.
col_names = ['dim_nk_cliente', 'dim_nm_cliente','dim_nm_email', 'dim_nr_phone', 'dim_is_current','dim_is_deleted', 'dim_starts_at', 'dim_ends_at', 'dim_inserted_at']

# Doing INSERT action in database
df_insert = df_merge.filter(df_merge.action == 'INSERT')\
                .select(
                    df_merge.contact_id.alias('dim_nk_cliente'),
                    df_merge.name_concat.alias('dim_nm_cliente'),
                    df_merge.email.alias('dim_nm_email'),
                    df_merge.phone.alias('dim_nr_phone'),
                    lit(1).alias('dim_is_current'),
                    lit(0).alias('dim_is_deleted'),
                    df_merge.starts_at.alias('dim_starts_at'),
                    df_merge.ends_at.alias('dim_ends_at'),
                    lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('dim_inserted_at')
                )

sql_insert = "INSERT INTO dim_contact (nk_cliente, nm_cliente, nm_email, nr_phone, is_current, is_deleted, starts_at, ends_at, inserted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
val_insert = [list(row.asDict().values()) for row in df_insert.collect()]
execute_action_db(database='dw_adventure', sql=sql_insert, val=val_insert)


# Doing DELETE action in database. Note that delete is a update statment, because we want to keep the old register.
df_delete = df_merge.filter(df_merge.action == 'DELETE')\
                .select(col_names)\
                .withColumn('dim_is_current', lit(0))\
                .withColumn('dim_is_deleted', lit(1))

sql_delete = "UPDATE dim_contact SET is_current = %s, is_deleted = %s, ends_at = %s WHERE nk_cliente = %s and starts_at = %s and ends_at = %s"
val_delete = [(False, True, current_date, row['dim_nk_cliente'], row['dim_starts_at'],  row['dim_ends_at'])  for row in df_delete.select('dim_nk_cliente', 'dim_starts_at', 'dim_ends_at').collect()]
execute_action_db(database='dw_adventure', sql=sql_delete, val=val_delete)

# UPSERT action have 2 actions: update old register and insert the new
# Doing a expire action
df_upsert_expire = df_merge.filter(df_merge.action == 'UPSERT').select(col_names)

sql_upsert_expire = "UPDATE dim_contact SET is_current = %s, is_deleted = %s, ends_at = %s WHERE nk_cliente = %s and starts_at = %s and ends_at = %s"
val_upsert_expire = [(False, False, current_date, row['dim_nk_cliente'], row['dim_starts_at'],  null_date)  for row in df_upsert_expire.select('dim_nk_cliente', 'dim_starts_at', 'dim_ends_at').collect()]
execute_action_db(database='dw_adventure', sql=sql_upsert_expire, val=val_upsert_expire)

# Doing a insert action
df_upsert_insert = df_merge.filter(df_merge.action == 'UPSERT')\
                    .select(
                        df_merge.contact_id.alias('dim_nk_cliente'),
                        df_merge.name_concat.alias('dim_nm_cliente'),
                        df_merge.email.alias('dim_nm_email'),
                        df_merge.phone.alias('dim_nr_phone'),
                        lit(1).alias('dim_is_current'),
                        lit(0).alias('dim_is_deleted'),
                        df_merge.starts_at.alias('dim_starts_at'),
                        df_merge.ends_at.alias('dim_ends_at'),
                        lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('dim_inserted_at')
                    )
sql_upsert_insert = "INSERT INTO dim_contact (nk_cliente, nm_cliente, nm_email, nr_phone, is_current, is_deleted, starts_at, ends_at, inserted_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
val_upsert_insert = [list(row.asDict().values()) for row in df_upsert_insert.collect()]
execute_action_db(database='dw_adventure', sql=sql_upsert_insert, val=val_upsert_insert)

print('********************************* Finish processing *********************************')
