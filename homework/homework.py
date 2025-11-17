"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
from zipfile import ZipFile
import glob
import pandas as pd
import os 

def client(dataFrame):

    #Select dataframe columns
    df_client = dataFrame[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()

    #Clean job column
    df_client["job"] = df_client["job"].str.replace(".","")
    df_client["job"] = df_client["job"].str.replace("-","_")

    #Clean education column
    df_client["education"] =df_client["education"].str.replace(".","_")
    df_client["education"] =df_client["education"].replace("unknown", pd.NA)

    #Clean credit_default column
    df_client["credit_default"] = df_client["credit_default"].apply(lambda x: 1 if x=="yes"else 0)

    #Clean mortage column
    df_client["mortgage"] = df_client["mortgage"].apply(lambda x: 1 if x=="yes" else 0)
    return df_client

def campaign(dataFrame):

    def month_to_number(month):
        month_dict = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12",
        }
        return month_dict.get(month.lower())

    #Select columns
    df_compaigns = dataFrame[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome"]].copy()

    #Clean previous_outcome column
    df_compaigns["previous_outcome"] =dataFrame["previous_outcome"].apply(lambda x: 1 if x =="success" else 0)
    #Clean campaign_outcome column
    df_compaigns["campaign_outcome"] =dataFrame["campaign_outcome"].apply(lambda x: 1 if x =="yes" else 0)

    #Create last_contact_day column
    df_compaigns["last_contact_date"]=(
        "2022-"
        +dataFrame["month"].apply(month_to_number)
        +"-"+dataFrame["day"].astype(str))
    
    return df_compaigns


def economics(dataFrame):
    #Select columns
    df_economics = dataFrame[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    return df_economics


def concatenate_dataframes(dataframes):
    return pd.concat(dataframes, ignore_index=True)


def create_output_directory(output_dir):
    if os.path.exists(output_dir):
        for file in glob.glob(f"{output_dir}/*"):
            os.remove(file)
        os.rmdir(output_dir)
    
    os.mkdir(output_dir)
    return output_dir


def get_info_files(rute):
  
    info = []
    # Iterate over all ZIP files matching the given path pattern
    for zip_file in glob.glob(rute):
        # Open the ZIP file for reading
        with ZipFile(zip_file, "r") as zip_file_1:
            # Get the name of the first file inside the ZIP (assumed to be the CSV)
            zinfo = zip_file_1.infolist()[0].filename
            # Open the CSV file inside the ZIP without extracting
            with zip_file_1.open(zinfo) as file:
                df = pd.read_csv(file)
                info.append(df)
    
    return info
                





def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    data_frames = get_info_files("files/input/*.zip")
    concatenated_df = concatenate_dataframes(data_frames)

    create_output_directory("files/output/")
    df_client = client(concatenated_df)
    df_client.to_csv(r"files/output/client.csv", index=False)
    df_campaign = campaign(concatenated_df)
    df_campaign.to_csv(r"files/output/campaign.csv", index=False)
    df_economics = economics(concatenated_df)
    df_economics.to_csv(r"files/output/economics.csv", index=False)
    


clean_campaign_data()
