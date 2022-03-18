import pandas as pd
from sqlalchemy import create_engine

def write_mysql(table_name, df):
    connection_url = r'mysql+pymysql://livingmap:wAps!06172019@livingmap.co.kr:3306/wmolcdb_new?charset=utf8'
    engine = create_engine(connection_url)
    try:
        with engine.begin() as conn:
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(">>> All good.")
    except Exception as e:
        print(">>> Something went wrong!")
    finally:
        print("\n")