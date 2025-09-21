import psycopg2

def check_hourly_data():
    # Connection parameters
    conn_params = {
        'dbname': 'ignitiondb',
        'user': 'ignition_user',
        'password': '1234567',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Execute query
        cur.execute("SELECT site, ts, temp, wind, rh_pct FROM risk_hourly LIMIT 5;")
        rows = cur.fetchall()
            
        print("\nRisk Hourly Data:")
        print("Site | Timestamp | Temperature | Wind | Humidity")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")

    except Exception as e:
        print(f"Error querying database: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_hourly_data()