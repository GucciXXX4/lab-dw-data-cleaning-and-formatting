import pandas as pd
import numpy as np

#dataset
def load_data(url: str) -> pd.DataFrame:
    return pd.read_csv(url)


#clean invalid values
def clean_invalid_values(df: pd.DataFrame) -> pd.DataFrame:  
    if "Gender" in df.columns:
        df["Gender"] = df["Gender"].str.strip().str.upper()
        df["Gender"] = df["Gender"].replace({
            "FEMALE": "F", "FEMAL": "F", "F": "F",
            "MALE": "M", "M": "M"
        })

 #fix state names
    if "State" in df.columns:
        df["State"] = df["State"].replace({
            "AZ": "Arizona", "Cali": "California", "WA": "Washington"
        })

 #fix Education
    if "Education" in df.columns:
        df["Education"] = df["Education"].replace({"Bachelors": "Bachelor"})

 #remove %
    if "Customer Lifetime Value" in df.columns:
        df["Customer Lifetime Value"] = df["Customer Lifetime Value"].str.replace("%", "", regex=False)

  #make easy vehicle class
    if "Vehicle Class" in df.columns:
        df["Vehicle Class"] = df["Vehicle Class"].replace({
            "Sports Car": "Luxury", "Luxury SUV": "Luxury", "Luxury Car": "Luxury"
        })

    return df


#format data types
def format_data_types(df: pd.DataFrame) -> pd.DataFrame:
  # conver to numeric
    if "Customer Lifetime Value" in df.columns:
        df["Customer Lifetime Value"] = pd.to_numeric(df["Customer Lifetime Value"], errors="coerce")

  #clean number of open complaints
    if "Number of Open Complaints" in df.columns:
        df["Number of Open Complaints"] = df["Number of Open Complaints"].astype(str).str.extract(r"(\d+)").astype(float)

    return df


#null values
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].fillna(df[col].median())

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna(df[col].mode()[0])

    return df


#duplicates
def handle_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates().reset_index(drop=True)
    return df


#main cleaning pipeline
def clean_pipeline(url: str) -> pd.DataFrame:
    df = load_data(url)
    df = clean_invalid_values(df)
    df = format_data_types(df)
    df = handle_nulls(df)
    df = handle_duplicates(df)
    return df
