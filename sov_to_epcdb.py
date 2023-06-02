import pandas as pd
import sqlalchemy as sa
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    text,
    Integer,
    insert,
    cast,
    update,
    Float,
    DateTime,
)
from sqlalchemy.sql import select, func
import numpy as np


def upload_solar_sov(sov_sheet, sql_engine) -> None:
    # upload the metadata
    readsheetname = "Solar SOV"
    df_master = pd.read_excel(sov_sheet, sheet_name=readsheetname, header=None)
    # change the other imports to use master.
    df = df_master.drop(df_master[df_master.index >= 19].index)
    df = df.iloc[:, :3]
    # df.rename(columns={'1': 'ColumnName', '2': 'ColumnValue'}, inplace=True)
    df.rename(columns={df.columns[1]: "ColumnName"}, inplace=True)
    df.rename(columns={df.columns[2]: "ColumnValue"}, inplace=True)
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df["ColumnValue"] = df["ColumnValue"].astype(str)
    # test.pivot(index=None, columns='t1',values='t2').bfill().iloc[[0],:]
    dfpivot = (
        df.pivot(index=None, columns="ColumnName", values="ColumnValue")
        .bfill()
        .iloc[[0], :]
    )
    dfpivot.rename(
        columns={
            "Project Name": "Project_Name",
            "Project Tracker ID": "Project_Tracker_ID",
            "Project Type": "Project_Type",
            "Scenario Name": "Scenario_Name",
            "Scenario ID": "Scenario_ID",
            "Estimate Source": "Estimate_Source",
            "Stage Gate": "Stage_Gate",
            "Milestone": "Milestone",
            "Design Package": "Design_Package",
            "EPE Version": "EPE_Version",
            "Buildable Land Version": "Buildable_Land_Version",
            "MW DC": "MW_DC",
            "MW AC": "MW_AC",
            "Module Count": "Module_Count",
            "Tracker Row Count": "Tracker_Row_Count",
            "Labor \n(Union/Prevailing/Non-Union)": "Labor_Union_Prevailing_Non_Union",
            "Contractor": "Contractor",
            "Date Submitted": "Date_Submitted",
        },
        inplace=True,
    )
    metadata = MetaData()
    table_name = "solar_projects"
    # Make sure that the Column names and types match with your DataFrame's columns
    table = Table(
        table_name,
        metadata,
        Column("Project_Name", String),
        Column("Project_Tracker_ID", Integer),
        Column("Project_Type", String),
        Column("Scenario_Name", String),
        Column("Scenario_ID", String),
        Column("Estimate_Source", String),
        Column("Stage_Gate", String),
        Column("Milestone", String),
        Column("Design_Package", String),
        Column("EPE_Version", String),
        Column("Buildable_Land_Version", String),
        Column("MW_DC", Float),
        Column("MW_AC", Float),
        Column("Module_Count", Float),
        Column("Tracker_Row_Count", Float),
        Column("Labor_Union_Prevailing_Non_Union", String),
        Column("Contractor", String),
        Column("Date_Submitted", DateTime),
        Column("id", Integer),
    )
    with sql_engine.connect() as connection:
        result = connection.execute(text("SELECT MAX(id) FROM solar_projects"))
        max_id = result.fetchone()[0]
    if max_id is None:
        max_id = 0
    droplist = dfpivot.columns.astype(str) == "nan"
    if droplist.any():
        dfpivot.drop(
            dfpivot.columns[[i for i, drop in enumerate(droplist) if drop]],
            axis=1,
            inplace=True,
        )
    dfpivot["id"] = max_id + 1
    # uploads the project
    dfpivot.to_sql(
        con=sql_engine,
        schema="dbo",
        name="solar_projects",
        if_exists="append",
        index=False,
        dtype={
            "Project_Name": String,
            "Project_Tracker_ID": Integer,
            "Project_Type": String,
            "Scenario_Name": String,
            "Scenario_ID": String,
            "Estimate_Source": String,
            "Stage_Gate": String,
            "Milestone": String,
            "Design_Package": String,
            "EPE_Version": String,
            "Buildable_Land_Version": String,
            "MW_DC": Float,
            "MW_AC": Float,
            "Module_Count": Float,
            "Tracker_Row_Count": Float,
            "Labor_Union_Prevailing_Non_Union": String,
            "Contractor": String,
            "Date_Submitted": DateTime,
            "id": Integer,
        },
    )
    # upload the costing
    # selects the first of the equivalent project ids, should be the last updated
    Project_Tracker_ID = dfpivot["Project_Tracker_ID"][0]
    query = text(
        """select id from  solar_projects where Project_Tracker_ID = {}""".format(
            Project_Tracker_ID, "{}"
        )
    )
    df = pd.read_sql_query(query, sql_engine)
    rid = df["id"].to_list()[-1]
    print(f"Upload ID: {rid}, options: {df['id'].iloc[:]}")
    df = df_master.drop(df_master[df_master.index <= 20].index)
    df.rename(columns={df.columns[0]: "Cost_Structure"}, inplace=True)
    df.rename(columns={df.columns[1]: "Description"}, inplace=True)
    df.rename(columns={df.columns[2]: "Quantity"}, inplace=True)
    df.rename(columns={df.columns[3]: "U_M"}, inplace=True)
    df.rename(columns={df.columns[4]: "Unit_Rate"}, inplace=True)
    df.rename(columns={df.columns[5]: "Extended_Price"}, inplace=True)
    df.rename(columns={df.columns[6]: "Price_per_Wp"}, inplace=True)
    df.rename(columns={df.columns[7]: "Comments"}, inplace=True)
    df.rename(columns={df.columns[8]: "Typical_Inclusions"}, inplace=True)
    df = df[df.Cost_Structure.notnull()]
    df["id"] = rid

    df.fillna(value=pd.NA, inplace=True)
    df.to_sql(
        con=sql_engine, schema="dbo", name="solar_sov", if_exists="append", index=False
    )
    print(f"{sov_sheet} successfully uploaded to Database")


def change_projid_to_integer(sql_engine):
    metadata = MetaData()

    # reflect the table
    table = Table("solar_projects", metadata, autoload_with=sql_engine)

    # create new column
    new_column = Column("Project_Tracker_ID_int", Integer, nullable=True)
    table.append_column(new_column)

    # add new column to the database
    with sql_engine.begin() as connection:
        connection.execute(
            "ALTER TABLE solar_projects ADD Project_Tracker_ID_int INTEGER"
        )

    # copy and cast data from the old column to the new one
    with sql_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE solar_projects SET Project_Tracker_ID_int = CAST(Project_Tracker_ID AS INTEGER)"
            )
        )

    # drop the old column
    with sql_engine.begin() as connection:
        connection.execute("ALTER TABLE solar_projects DROP COLUMN Project_Tracker_ID")

    # rename the new column to the old column's name
    with sql_engine.begin() as connection:
        connection.execute(
            "EXEC sp_rename 'solar_projects.Project_Tracker_ID_int', 'Project_Tracker_ID', 'COLUMN';"
        )
    print("Project_id successfully cast to INT")


def excel_epc_sov_to_db(sov_sheet, sql_engine) -> None:
    # Use the sql_engine t the appropriate columns to the sql server

    # the same list of columns as before
    # new_columns = [
    #     "Project_Tracker_ID",
    # ]
    # # for each column, create a new ALTER TABLE command
    # for col_name in new_columns:
    #     with sql_engine.begin() as connection:
    #         connection.execute(
    #             text(f"ALTER TABLE dbo.solar_projects ADD [{col_name}] NVARCHAR(MAX);")
    #         )
    # retrieves the template to be used for the EPC-DB
    readsheetname = "Solar SOV"
    df_master = pd.read_excel(sov_sheet, sheet_name=readsheetname, header=None)
    # change the other imports to use master.
    df = df_master.drop(df_master[df_master.index >= 19].index)
    df = df.iloc[:, :3]
    # df.rename(columns={'1': 'ColumnName', '2': 'ColumnValue'}, inplace=True)
    df.rename(columns={df.columns[1]: "ColumnName"}, inplace=True)
    df.rename(columns={df.columns[2]: "ColumnValue"}, inplace=True)
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df["ColumnValue"] = df["ColumnValue"].astype(str)
    # test.pivot(index=None, columns='t1',values='t2').bfill().iloc[[0],:]
    dfpivot = (
        df.pivot(index=None, columns="ColumnName", values="ColumnValue")
        .bfill()
        .iloc[[0], :]
    )
    dfpivot.rename(
        columns={
            "Project Name": "Project_Name",
            "Project Tracker ID": "Project_Tracker_ID",
            "Project Type": "Project_Type",
            "Scenario Name": "Scenario_Name",
            "Scenario ID": "Scenario_ID",
            "Estimate Source": "Estimate_Source",
            "Stage Gate": "Stage_Gate",
            "Milestone": "Milestone",
            "Design Package": "Design_Package",
            "EPE Version": "EPE_Version",
            "Buildable Land Version": "Buildable_Land_Version",
            "MW DC": "MW_DC",
            "MW AC": "MW_AC",
            "Module Count": "Module_Count",
            "Tracker Row Count": "Tracker_Row_Count",
            "Labor \n(Union/Prevailing/Non-Union)": "Labor_Union_Prevailing_Non_Union",
            "Contractor": "Contractor",
            "Date Submitted": "Date_Submitted",
        },
        inplace=True,
    )
    droplist = dfpivot.columns.astype(str) == "nan"
    if droplist.any():
        dfpivot.drop(
            dfpivot.columns[[i for i, drop in enumerate(droplist) if drop]],
            axis=1,
            inplace=True,
        )
    print()
    conn_db = sql_engine
    # dfpivot = dfpivot["Project_Tracker_ID"]
    # dfpivot.to_sql(
    #     con=conn_db,
    #     schema="dbo",
    #     name="solar_projects",
    #     if_exists="append",
    #     index=False,
    # )
    #
    # # add the "typical inclusions" column
    # with sql_engine.begin() as connection:
    #     connection.execute(
    #         text("ALTER TABLE dbo.solar_sov ADD [Typical_Inclusions] NVARCHAR(MAX);")
    #     )

    Project_Tracker_ID = dfpivot["Project_Tracker_ID"][0]
    query = text(
        """select id from  solar_projects where Project_Tracker_ID = {}""".format(
            Project_Tracker_ID, "{}"
        )
    )
    df = pd.read_sql_query(query, conn_db)
    rid = df["id"][0]
    df = df_master.drop(df_master[df_master.index <= 20].index)
    df.rename(columns={df.columns[0]: "Cost_Structure"}, inplace=True)
    df.rename(columns={df.columns[1]: "Description"}, inplace=True)
    df.rename(columns={df.columns[2]: "Quantity"}, inplace=True)
    df.rename(columns={df.columns[3]: "U_M"}, inplace=True)
    df.rename(columns={df.columns[4]: "Unit_Rate"}, inplace=True)
    df.rename(columns={df.columns[5]: "Extended_Price"}, inplace=True)
    df.rename(columns={df.columns[6]: "Price_per_Wp"}, inplace=True)
    df.rename(columns={df.columns[7]: "Comments"}, inplace=True)
    df.rename(columns={df.columns[8]: "Typical_Inclusions"}, inplace=True)
    df = df[df.Cost_Structure.notnull()]
    df["id"] = rid

    df.fillna(value=pd.NA, inplace=True)
    df.to_sql(
        con=conn_db, schema="dbo", name="solar_sov", if_exists="replace", index=False
    )
    print("SQL Uploaded")
    ## Now for Storage
    readsheetname = "Storage SOV"
    df_master = pd.read_excel(sov_sheet, sheet_name=readsheetname, header=None)
    df = df_master.drop(df_master[df_master.index >= 23].index)
    df = df.iloc[:, :3]
    df.rename(columns={df.columns[1]: "ColumnName"}, inplace=True)
    df.rename(columns={df.columns[2]: "ColumnValue"}, inplace=True)
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df["ColumnValue"] = df["ColumnValue"].astype(str)
    dfpivot = (
        df.pivot(index=None, columns="ColumnName", values="ColumnValue")
        .bfill()
        .iloc[[0], :]
    )

    dfpivot.rename(
        columns={
            "Project Name": "Project_Name",
            "Project Tracker ID": "Project_Tracker_ID",
            "Project Type": "Project_Type",
            "Scenario Name": "Scenario_Name",
            "Scenario ID": "Scenario_ID",
            "Estimate Source": "Estimate_Source",
            "Stage Gate": "Stage_Gate",
            "Milestone": "Milestone",
            "Design Package": "Design_Package",
            "EPE Version": "EPE_Version",
            "Buildable Land Version": "Buildable_Land_Version",
            "BESS OEM": "BESS_OEM",
            "Product Type": "Product_Type",
            "Coupling": "Coupling",
            "Battery Size at POI[MW]": "Battery_Size_at_POI[MW]",
            "Discharge Duration[hr]": "Discharge_Duration[hr]",
            "MWh Installed": "MWh_Installed",
            "BESS Container Quantity": "BESS_Container_Quantity",
            "PCS Quantity": "PCS_Quantity",
            "Labor (Union/Prevailing/Non-Union)": "Labor_Union_Prevailing_Non_Union",
            "Contractor": "Contractor",
            "Date Submitted": "Date_Submitted",
        },
        inplace=True,
    )
    droplist = dfpivot.columns.astype(str) == "nan"
    if droplist.any():
        dfpivot.drop(
            dfpivot.columns[[i for i, drop in enumerate(droplist) if drop]],
            axis=1,
            inplace=True,
        )
    dfpivot.to_sql(
        con=conn_db,
        schema="dbo",
        name="Storage_projects",
        if_exists="append",
        index=False,
    )
    # Project_Tracker_ID = dfpivot["Project_Tracker_ID"][0]
    # # gets the most current id based on project.
    # query = (
    #     """select id from  vw_Storage_projects where Project_Tracker_ID = {}""".format(
    #         Project_Tracker_ID, "{}"
    #     )
    # )
    # df = pd.read_sql_query(query, conn_db)
    # rid = df["id"][0]
    # # df.drop(df[df.index <= 19].index, inplace=True)
    # df = df_master.drop(df_master[df_master.index <= 24].index)
    # df.rename(columns={df.columns[0]: "Cost_Structure"}, inplace=True)
    # df.rename(columns={df.columns[1]: "Description"}, inplace=True)
    # df.rename(columns={df.columns[2]: "Quantity"}, inplace=True)
    # df.rename(columns={df.columns[3]: "U_M"}, inplace=True)
    # df.rename(columns={df.columns[4]: "Unit_Rate"}, inplace=True)
    # df.rename(columns={df.columns[5]: "Extended_Price"}, inplace=True)
    # df.rename(columns={df.columns[6]: "Price_per_kWh"}, inplace=True)
    # df.rename(columns={df.columns[7]: "Comments"}, inplace=True)
    # df = df[df.Cost_Structure.notnull()]
    # df["id"] = rid
    # df.to_sql(
    #     con=conn_db, schema="dbo", name="storage_sov", if_exists="append", index=False
    # )

    # Now for HV
    # first create the appropriate fields in the database
    # list of new columns to be added (as per your previous sheet structure)
    new_columns_sov = [
        "Cost_Structure",
        "Description",
        "Quantity",
        "U_M",
        "Unit_Rate",
        "Extended_Price",
        "Price_per_Wp",
        "Comments",
        "id",
    ]
    new_columns_projects = [
        "Project_Name",
        "Project_Tracker_ID",
        "Project_Type",
        "Scenario_Name",
        "Scenario_ID",
        "Estimate_Source",
        "Stage_Gate",
        "Milestone",
        "Design_Package",
        "EPE_Version",
        "Buildable_Land_Version",
        "MW_AC",
        "Interconnect_Voltage",
        "Labor_Union_Prevailing_Non_Union",
        "Contractor",
        "Date_submitted",
    ]

    # Create tables and columns
    with sql_engine.begin() as connection:
        # Create "hv_projects" table
        connection.execute(
            text("CREATE TABLE dbo.hv_projects (Project_Tracker_ID NVARCHAR(MAX));")
        )
        for col_name in new_columns_projects:
            connection.execute(
                text(f"ALTER TABLE dbo.hv_projects ADD [{col_name}] NVARCHAR(MAX);")
            )

        # Create "hv_sov" table
        connection.execute(text("CREATE TABLE dbo.hv_sov (id INT);"))
        for col_name in new_columns_sov:
            connection.execute(
                text(f"ALTER TABLE dbo.hv_sov ADD [{col_name}] NVARCHAR(MAX);")
            )

    # retrieves the template to be used for the EPC-DB
    readsheetname = "HV SOV"
    df_master = pd.read_excel(sov_sheet, sheet_name=readsheetname, header=None)
    # change the other imports to use master.
    df = df_master.drop(df_master[df_master.index >= 17].index)
    df = df.iloc[:, :3]
    # df.rename(columns={'1': 'ColumnName', '2': 'ColumnValue'}, inplace=True)
    df.rename(columns={df.columns[1]: "ColumnName"}, inplace=True)
    df.rename(columns={df.columns[2]: "ColumnValue"}, inplace=True)
    df.drop(df.columns[[0, 3, 4, 5, 6, 7]], axis=1, inplace=True)
    df["ColumnValue"] = df["ColumnValue"].astype(str)
    # test.pivot(index=None, columns='t1',values='t2').bfill().iloc[[0],:]
    dfpivot = (
        df.pivot(index=None, columns="ColumnName", values="ColumnValue")
        .bfill()
        .iloc[[0], :]
    )
    dfpivot.rename(
        columns={
            "Project Name": "Project_Name",
            "Project Tracker ID": "Project_Tracker_ID",
            "Project Type": "Project_Type",
            "Scenario Name": "Scenario_Name",
            "Scenario ID": "Scenario_ID",
            "Estimate Source": "Estimate_Source",
            "Stage Gate": "Stage_Gate",
            "Milestone": "Milestone",
            "Design Package": "Design_Package",
            "EPE Version": "EPE_Version",
            "Buildable Land Version": "Buildable_Land_Version",
            "MW AC": "MW_AC",
            "Interconnect Voltage": "Interconnect_Voltage",
            "Labor \n(Union/Prevailing/Non-Union)": "Labor_Union_Prevailing_Non_Union",
            "Contractor": "Contractor",
            "Date Submitted": "Date_Submitted",
        },
        inplace=True,
    )
    droplist = dfpivot.columns.astype(str) == "nan"
    if droplist.any():
        dfpivot.drop(
            dfpivot.columns[[i for i, drop in enumerate(droplist) if drop]],
            axis=1,
            inplace=True,
        )
    conn_db = sql_engine
    dfpivot.to_sql(
        con=conn_db,
        schema="dbo",
        name="hv_projects",
        if_exists="append",
        index=False,
    )
    # Project_Tracker_ID = dfpivot["Project_Tracker_ID"][0]
    # query = (
    #     """select id from  vw_solar_projects where Project_Tracker_ID = {}""".format(
    #         Project_Tracker_ID, "{}"
    #     )
    # )
    # df = pd.read_sql_query(query, conn_db)
    # rid = df["id"][0]
    # df = df_master.drop(df_master[df_master.index <= 18].index)
    # df.rename(columns={df.columns[0]: "Cost_Structure"}, inplace=True)
    # df.rename(columns={df.columns[1]: "Description"}, inplace=True)
    # df.rename(columns={df.columns[2]: "Quantity"}, inplace=True)
    # df.rename(columns={df.columns[3]: "U_M"}, inplace=True)
    # df.rename(columns={df.columns[4]: "Unit_Rate"}, inplace=True)
    # df.rename(columns={df.columns[5]: "Extended_Price"}, inplace=True)
    # df.rename(columns={df.columns[6]: "Price_per_Wp"}, inplace=True)
    # df.rename(columns={df.columns[7]: "Comments"}, inplace=True)
    # df = df[df.Cost_Structure.notnull()]
    # df["id"] = rid
    # df.to_sql(con=conn_db, schema="dbo", name="hv_sov", if_exists="append", index=False)
    print()


def db_conn_get():
    driver = "ODBC+Driver+17+for+SQL+Server"
    database = "EPC_SOV"
    server = "sdhqhopsql01d"
    conn_string = "mssql+pyodbc://{srv}/{db}?trusted_connection=yes&driver={dr}".format(
        dr=driver, srv=server, db=database
    )
    engine = sa.create_engine(conn_string)
    return engine


def add_id_column(sql_engine):
    metadata = MetaData()
    my_table = Table("solar_projects", metadata, autoload_with=sql_engine)

    # Add new column 'id'
    with sql_engine.begin() as connection:
        connection.execute("ALTER TABLE solar_projects ADD id INTEGER")

    # Set the new 'id' column to 0 for one row
    with sql_engine.begin() as connection:
        connection.execute("UPDATE solar_projects SET id = 0")


if __name__ == "__main__":
    sov_fdir = "C:\\Users\\nils.rundquist\\PycharmProjects\\pythonProject\\"
    sov = "Milagro_Rosendin_2023-03-01.xlsx"
    conn_db = db_conn_get()
    # add_id_column(conn_db)
    upload_solar_sov(sov, conn_db)
