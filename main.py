import os
import pysftp
import pandas as pd
from sqlalchemy import create_engine


def main():
    database_url = os.getenv("DATABASE_URL")
    db_schema = os.getenv("DB_SCHEMA", None)
    engine = create_engine(database_url)

    if os.getenv("STMATH_ENABLED") == "YES":
        upload_stmath(engine, db_schema)

    if os.getenv("CLASSLINK_ENABLED") == "YES":
        upload_classlink(engine, db_schema)

    if os.getenv("ACHIEVE3000_ENABLED") == "YES":
        upload_achieve3000(engine, db_schema)


def upload_classlink(engine, db_schema):
    print("Uploading data from Classlink...", flush=True)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(".known_hosts")
    with pysftp.Connection(
        os.getenv("CLASSLINK_SFTP_HOST"),
        username=os.getenv("CLASSLINK_SFTP_USERNAME"),
        password=os.getenv("CLASSLINK_SFTP_PASSWORD"),
        cnopts=cnopts,
    ) as sftp:
        print("Connected to FTP server", flush=True)
        sftp.chdir(os.getenv("CLASSLINK_PATH_TO_FILES"))
        file_list = sftp.listdir()
        print(f"Found {len(file_list)} total files.", flush=True)

        for file_type in ["appLaunchesRawLog", "appTimeTrackingRawLog"]:
            filtered_files = [f for f in file_list if file_type in f]
            print(f"Found {len(filtered_files)} files for {file_type}.", flush=True)
            total_df = pd.DataFrame()
            for f in filtered_files:
                with sftp.open(f) as new_file:
                    print(f"Loading file {f}", flush=True)
                    df = pd.read_csv(new_file)
                    total_df = pd.concat([total_df, df])

            total_df = total_df.drop_duplicates(ignore_index=True)
            print(f"Found {len(total_df.index)} records to upload.", flush=True)
            table_name = f"Classlink_{file_type}"
            total_df.to_sql(
                table_name,
                con=engine,
                if_exists="replace",
                schema=db_schema,
            )
            print(f"Successfully uploaded to {table_name}.", flush=True)


def upload_stmath(engine, db_schema):
    print("Uploading data from STMath...")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(".known_hosts")
    with pysftp.Connection(
        os.getenv("STMATH_SFTP_HOST"),
        username=os.getenv("STMATH_SFTP_USERNAME"),
        password=os.getenv("STMATH_SFTP_PASSWORD"),
        cnopts=cnopts,
    ) as sftp:
        print("Connected to FTP server", flush=True)
        sftp.chdir(os.getenv("STMATH_PATH_TO_FILES"))
        files = sftp.listdir()
        file_list = [
            f for f in files if f.startswith("weeklyFiles") and f.endswith(".csv")
        ]
        last_weekly_file = sorted(file_list)[-1]
        print(f"Found last weekly file: {last_weekly_file}", flush=True)
        with sftp.open(last_weekly_file) as new_file:
            df = pd.read_csv(new_file)
            print(f"Found {len(df.index)} records to upload.", flush=True)
            df.to_sql(
                "STMath_Summary",
                con=engine,
                if_exists="replace",
                schema=db_schema,
            )
            print("Successfully uploaded records.", flush=True)


def upload_achieve3000(engine, db_schema):
    print("Uploading data from Achieve3000 and SmartyAnts...")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(".known_hosts")
    with pysftp.Connection(
        os.getenv("ACHIEVE3000_SFTP_HOST"),
        username=os.getenv("ACHIEVE3000_SFTP_USERNAME"),
        password=os.getenv("ACHIEVE3000_SFTP_PASSWORD"),
        cnopts=cnopts,
    ) as sftp:
        print("Connected to FTP server", flush=True)
        sftp.chdir(os.getenv("ACHIEVE3000_PATH_TO_FILES"))
        files = sftp.listdir()
        file_list = [f for f in files if f.endswith("student-byClass.csv")]
        latest_file = sorted(file_list)[-1]
        print(f"Found latest file: {latest_file}", flush=True)
        with sftp.open(latest_file) as new_file:
            df = pd.read_csv(new_file)
            print(f"Found {len(df.index)} records to upload.", flush=True)
            df.to_sql(
                "Achieve3000_Summary",
                con=engine,
                if_exists="replace",
                schema=db_schema,
            )
            print("Successfully uploaded records.", flush=True)


if __name__ == "__main__":
    main()
