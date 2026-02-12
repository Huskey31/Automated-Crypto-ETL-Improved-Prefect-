import pandas as pd
from sqlalchemy import create_engine

def test_data():
    df = pd.read_csv(r"C:\Users\khany\OneDrive\Desktop\Backup_postgres\coin_gecko.csv")
    m_values = df.isnull().sum()
    c_duplicate = df.duplicated().any()
    #changing date and time to datetime format
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])
    #sorting date and time in order
    df = df.sort_values("extracted_at")
    #creating a new row time_diff to check accuracy of extraction (every 30minutes)
    df["time_diff"] = df.groupby("Coin")["extracted_at"].diff()
    d_accuracy = df.groupby("Coin")["time_diff"].mean()
    tot_acc = df["time_diff"].dropna().mean()
    no_null = m_values.sum() == 0

    print(f"your missing values are: {m_values} ")
    print(f"testing data for duplicates: {c_duplicate} ")
    print(f"Checking if data is within testing range: {tot_acc} ")

    if no_null and c_duplicate == False and pd.Timedelta(minutes=30) < tot_acc < pd.Timedelta(minutes=35):
        print("Data is ready for testing ")
    else:
        print("Data is not ready for testing, please check your data and try again")

if __name__=="__main__":
    test_data()



