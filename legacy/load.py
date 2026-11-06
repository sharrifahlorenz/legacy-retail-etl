import os
import sqlite3

from legacy import config


def load_table(df, table_name):
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(config.WAREHOUSE_DB)
    try:
        # full truncate-and-reload every night, no history kept
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()
    # analysts also pull CSVs off the shared drive
    df.to_csv(os.path.join(config.OUTPUT_DIR, table_name + ".csv"), index=False)
