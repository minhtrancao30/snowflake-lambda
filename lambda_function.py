import os
import snowflake.connector

def lambda_handler(event, context):
    snowflake_config = {
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA']
    }

    try: 
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER();")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
    
        return {
            'statusCode': 200,
            'body': f"Connected to Snowflake as user: {result[0]}"
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error connecting to Snowflake: {str(e)}"
        }