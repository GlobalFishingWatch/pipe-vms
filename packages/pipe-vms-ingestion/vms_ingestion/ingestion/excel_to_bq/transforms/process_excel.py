import pandas as pd


# Function to convert DMS to decimal
def dms_to_decimal(dms_str):
    dms_str = dms_str.strip()
    degrees, rest = dms_str.split("°")
    minutes, rest = rest.split("'")
    seconds, direction = rest.split('" ')
    degrees = float(degrees)
    minutes = float(minutes)
    seconds = float(seconds)
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ["S", "W"]:
        decimal = -decimal
    return decimal


def extract_float(df_val):
    df_val = df_val.strip()
    if df_val == "-":
        return None
    value, _ = df_val.split(" ")
    return float(value)


def process_excel(file_content):
    # This function processes each Excel file and yields dictionaries for BigQuery
    df = pd.read_excel(file_content)

    if "Latitud" in df.columns:
        df["Latitud"] = df["Latitud"].apply(dms_to_decimal)
    if "Longitud" in df.columns:
        df["Longitud"] = df["Longitud"].apply(dms_to_decimal)
    if "Rumbo" in df.columns:
        df["Rumbo"] = df["Rumbo"].apply(extract_float)
    if "Velocidad" in df.columns:
        df["Velocidad"] = df["Velocidad"].apply(extract_float)
    if "Fecha de la posición" in df.columns and "Hora de la posición" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["Fecha de la posición"] + " " + df["Hora de la posición"],
            format="%d/%m/%Y %H:%M",
        )
        df.drop(["Fecha de la posición", "Hora de la posición"], axis=1, inplace=True)

    df["shipname"] = df["Nombre de la nave"]
    df["lat"] = df["Latitud"]
    df["lon"] = df["Longitud"]
    df["speed"] = df["Velocidad"]
    df["heading"] = df["Rumbo"]
    df.drop(
        ["Nombre de la nave", "Latitud", "Longitud", "Velocidad", "Rumbo"],
        axis=1,
        inplace=True,
    )

    # Yielding each row as a dictionary for BigQuery
    for _, row in df.iterrows():
        yield row.to_dict()
