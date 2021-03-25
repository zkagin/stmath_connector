import os
import pysftp
import pandas as pd
from sqlalchemy import create_engine


def main():
    database_url = os.getenv("DATABASE_URL")
    db_schema = os.getenv("DB_SCHEMA", None)
    engine = create_engine(database_url)

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(".known_hosts")
    with pysftp.Connection(
        os.getenv("SFTP_HOST"),
        username=os.getenv("SFTP_USERNAME"),
        password=os.getenv("SFTP_PASSWORD"),
        cnopts=cnopts,
    ) as sftp:
        print("Connected to FTP server", flush=True)
        sftp.chdir(os.getenv("SFTP_INITIAL_DIR"))
        sftp.chdir("home")
        sftp.chdir("Reports")
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


if __name__ == "__main__":
    main()
