import psycopg2

def run_sql_script(script_path):
    # Connection parameters
    conn_params = {
        'dbname': 'ignitiondb',
        'user': 'postgres',
        'password': '1234567',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # We want each statement to be executed immediately
        cur = conn.cursor()

        # Read and execute the SQL script
        with open(script_path, 'r') as f:
            sql_script = f.read()
            cur.execute(sql_script)
            
        print("Permissions script executed successfully.")

    except Exception as e:
        print(f"Error executing permissions script: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_sql_script('setup_db_permissions.sql')