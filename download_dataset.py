"""
Query the production database and
download the images from Azure BLOB storage.
"""

import os
import struct
from azure.storage.blob import BlobServiceClient
import pandas as pd
import pyodbc
from tqdm import tqdm

def handle_datetimeoffset(dto_value):
    """Datetime decoder for ODBC SQL."""
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

def main():
    """
    Main module script.
    """

    # database credentials
    db_user = os.environ.get('AZURE_SQL_USER')
    db_pwd = os.environ.get('AZURE_SQL_PWD')
    db_dvr = 'ODBC Driver 17 for SQL Server'
    db_svr = 'tcp:ar-database-thorlabs.database.windows.net,1433'
    db_name = 'tsw_web_production'

    # create the connection string
    cxn_str = f"DRIVER={db_dvr};SERVER={db_svr};DATABASE={db_name};UID={db_user};PWD={db_pwd}"

    # create connection and curser object
    cxn = pyodbc.connect(cxn_str)
    cxn.add_output_converter(-155, handle_datetimeoffset)
    cur = cxn.cursor()

    # create query
    sql_str = (
        " SELECT part.[id],part.[serial],part.[index],part.[created],part.[modified],part.[notes],part.[fail1_object_id],"
        " part.[image_data_pre],part.[image_data_post],part.[sem_data],part.[uv_vis_data_post],part.[ir_data_post],"
        " batch.id as batch_id, batch.name as batch_name, batch.sku_object_id as batch_sku_id,"
        " opt.name as sku_name,  opt.min_diameter_mm as sku_od_mm"
        " FROM [dbo].[texturedar_part] as part"
        " INNER JOIN [dbo].[texturedar_tarbatch] as batch"
        " ON batch.id = part.[batch_object_id]"
        " INNER JOIN [dbo].[texturedar_opticalcoatsku] as opt"
        " ON opt.id = batch.sku_object_id"
        " WHERE part.[image_data_post] IS NOT NULL"
        " AND part.[image_data_post] NOT IN ('')"
        " ORDER BY part.[id] ASC;"
    )

    cur.execute(sql_str)

    data = cur.fetchall()

    # save the query results to file
    col_headers = [h[0] for h in cur.description]
    df = pd.DataFrame([list(row) for row in data], columns=col_headers)
    df.to_csv("./data/query_results_.csv", index=False)

    # configure azure blob credentials
    az_endpoints = 'https'
    az_acct = 'tsw'
    az_key = os.environ.get('AZURE_ACCOUNT_KEY')
    az_suffix = 'core.windows.net'

    # azure blob client connection string
    az_cxn_str = (f"DefaultEndpointsProtocol={az_endpoints};AccountName={az_acct};"
                  f"AccountKey={az_key};EndpointSuffix={az_suffix}")

    # blob client & 'media' container
    az_blob_client = BlobServiceClient.from_connection_string(az_cxn_str)
    ##az_container = az_blob_client.get_container_client('media')

    # download the images from blob
    for row in tqdm(data):
        if row.image_data_post:
            az_get_blob = az_blob_client.get_blob_client('media', row.image_data_post)
            dl_path = os.path.join(
                './data/downloads',
                str(row.image_data_post).replace('ar_data/production/image_data/post/', ''))

            if not os.path.exists(dl_path):
                with open(dl_path, 'wb') as outfile:
                    outfile.write(az_get_blob.download_blob().readall())

if __name__ == "__main__":
    main()
