import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO
import os
import csv


def read_file(file_path):
    try:
        return pd.read_csv(
            file_path,
            delimiter="|",
            encoding="ISO-8859-1",
            on_bad_lines="skip",
            dtype=str,
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            file_path, delimiter="|", encoding="cp1252", on_bad_lines="skip", dtype=str
        )


def preprocess_df(df):
    df.replace({r"\r": " ", r"\n": " ", r"\r\n": " "}, regex=True, inplace=True)
    df.replace({'"': r"\""}, regex=True, inplace=True)


def connect_db():
    return psycopg2.connect(
        dbname="postgres",
        user="clinicaltrials",
        password="clinicaltrials",
        host="localhost",
    )


def create_table(df, table_name, conn):
    cursor = conn.cursor()
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
    )
    df.columns = [
        f'"{col}"' if " " in col or col.isdigit() else col for col in df.columns
    ]
    str_cols = ",".join([f"{col} VARCHAR" for col in df.columns])
    cursor.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name), sql.SQL(str_cols)
        )
    )
    conn.commit()
    cursor.close()


def load_data(df, table_name, conn):
    buffer = StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        sep="|",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_expert(
            f"COPY {table_name} FROM STDIN WITH CSV DELIMITER '|' NULL '' ESCAPE '\\' QUOTE '\"'",
            buffer,
        )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    print(f"Data loaded successfully into {table_name}")


def load_problematic_data(df, table_name, conn):
    tmp_csv = "tmp.csv"

    df.to_csv(
        tmp_csv,
        index=False,
        header=False,
        sep="|",
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
    )

    with open(tmp_csv, "r") as f:
        cursor = conn.cursor()
        try:
            cursor.copy_expert(
                "COPY {} FROM STDIN WITH CSV DELIMITER '|'".format(table_name), f
            )
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: ", error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    os.remove(tmp_csv)

    print("Data loaded successfully into {}".format(table_name))


def get_data(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
    records = cursor.fetchall()
    cursor.close()
    return records


def process_and_load(file_path, table_name, conn):
    df = read_file(file_path)
    preprocess_df(df)
    create_table(df, table_name, conn)

    problematic_files = {
        "registration": "registration",
        "owner_operator": "owner_operator",
        "non_reg_imp_id_by_manu": "non_reg_imp_id_by_manu",
        "listing_proprietary_name": "listing_proprietary_name",
        "contact_addresses": "contact_addresses",
    }

    if table_name in problematic_files:
        load_problematic_data(df, table_name, conn)
    else:
        load_data(df, table_name, conn)


def main():
    conn = connect_db()

    file_table_map = {
        "us_agent.txt": "us_agent",
        "Registration.txt": "registration",
        "registration_listing.txt": "registration_listing",
        "Reg_Imp_ID_by_Manu.txt": "reg_imp_id_by_manu",
        "Owner_Operator.txt": "owner_operator",
        "Official_Correspondent.txt": "official_correspondent",
        "Non_Reg_Imp_ID_by_Manu.txt": "non_reg_imp_id_by_manu",
        "Manu_ID_by_Imp.txt": "manu_id_by_imp",
        "Listing_Proprietary_Name.txt": "listing_proprietary_name",
        "Listing_PCD.txt": "listing_pcd",
        "listing_estabtypes.txt": "listing_estabtypes",
        "estabtypes.txt": "estabtypes",
        "contact_addresses.txt": "contact_addresses",
    }

    for file_path, table_name in file_table_map.items():
        process_and_load(file_path, table_name, conn)

    data = get_data("us_agent", conn)


if __name__ == "__main__":
    os.chdir("../Data/openfda/OtherDownloads/")
    main()
