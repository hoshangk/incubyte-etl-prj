from sql_db_connection import connection, cursor
import pandas as pd

cursor = connection.cursor(buffered=True)

def main():

    # Reading file from csv file
    patients_df = pd.read_csv("Data/patients.csv", sep="|")
    # print(patients_df)

    patients_df.drop(patients_df.columns[[0, 1]], axis = 1, inplace = True)
    # print(patients_df)

    # cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    all_tables= []
    for table_name in cursor:
        table,  = table_name
        all_tables.append(table)

    print(all_tables)
    countries = patients_df['Country'].unique()
    # print(countries)
    for _country in countries:
        if _country.lower() in all_tables:
            temp_patient_df = patients_df[patients_df['Country'] == _country]
            insert_data(_country, temp_patient_df)

        else:
            create_table(_country)

def create_table(country):
    """
    Function To Create Table for a Particular country
    :param country: country name
    :return:
    """
    mySql_Create_Table_Query = """CREATE TABLE `incubyte`.`{country}`  ( `Customer_Name` VARCHAR(255) NOT NULL , 
                                    `Customer_ID` VARCHAR(18) NOT NULL , `Customer_Open_Date` DATE NOT NULL , 
                                    `Last_Consulted_Date` DATE NULL , `Vaccination_Type` CHAR(5) NULL , 
                                    `Doctor_Consulted` CHAR(255) NULL , `State` CHAR(5) NULL , 
                                    `Country` CHAR(5) NULL , `Post_Code` INT(5) NULL , 
                                    `Date_of_Birth` DATE NULL , `Active_Customer` CHAR(1) NULL , 
                                    PRIMARY KEY (`Customer_ID`), INDEX (`Customer_Open_Date`), 
                                    INDEX (`Customer_Name`)) ENGINE = InnoDB; """.format(vars="variables", country=country)

    print(mySql_Create_Table_Query)
    cursor.execute(mySql_Create_Table_Query)
    print(f"{country} Table created successfully ")

    return True

def insert_data(country, data):
    """
    Function to Insert data into the respective country table
    :param country:
    :param data:
    :return:
    """
    table_column_list = ["Customer_Name", "Customer_Id", "Customer_Open_Date", "Last_Consulted_Date", "Vaccination_Type",
                         "Doctor_Consulted", "State", "Country", "Date_of_Birth", "Active_Customer"]

    print(data)
    # exit()
    cols = "`,`".join([str(i) for i in table_column_list])
    # Insert DataFrame recrds one by one.
    for i, row in data.iterrows():

        # print(row)
        # Check if Corresponding CustomerId is already present
        Customer_Id = row.Customer_Id
        # print(Customer_Id)
        chk_sql = f"select Customer_ID from `{country}` where `Customer_ID` = '{Customer_Id}'"
        print(chk_sql)
        cursor.execute(chk_sql)
        rows = cursor.rowcount
        # rows,  = cursor
        print("rows-", rows)
        if rows != -1:
            print("CustomerId Already Exists")

        else:
            sql = f"INSERT INTO `{country}` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
            print(sql)
            cursor.execute(sql, tuple(row))

            # the connection is not autocommitted by default, so we must commit to save our changes
            connection.commit()
    # connection.close()
    return True



if __name__ == '__main__':
    main()