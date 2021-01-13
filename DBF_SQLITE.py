# This program takes 3 provided shapefiles from vic spatial data sources, reads the shapefiles and outputs each record
# to the SQLite database file and creates relevant indexes.
#
# Created by Tyler McCamley 09/2019

# USING DBF_MSSQL INSTEAD - THIS IS ONLY FOR HISTORICAL PURPOSES

'''
import sqlite3
import sys
from DBF_Read import records
from DBF_Read import shape_records

database_name = 'VICMAP_DATA.db'

connection = sqlite3.connect(database_name)
cursor = connection.cursor()


def parcel_property_join_table():
    # PARCEL PROPERTY JOIN TABLE
    print('Beginning PARCEL_PROPERTY IMPORT:\n')
    try:
        cursor.execute("DROP TABLE PARCEL_PROPERTY")
        print('Table PARCEL_PROPERTY Dropped')
    except sqlite3.OperationalError:
        cursor.execute("CREATE TABLE PARCEL_PROPERTY (PARCEL_PFI, PR_PFI, UFI)")
        print('Table PARCEL_PROPERTY Created')

    try:
        cursor.execute("CREATE TABLE PARCEL_PROPERTY (PARCEL_PFI, PR_PFI, UFI)")
        print('Table PARCEL_PROPERTY Created')
    except sqlite3.OperationalError:
        pass

    print('Importing data into: PARCEL_PROPERTY, this may take a while please wait...')
    line_count = 1

    for line in records('Shapefiles/PARCEL_PROPERTY.dbf'):
        print(f'\r  Records imported: {line_count}', end='', flush=True)
        parcel_pfi = str(line['PARCEL_PFI'])
        pr_pfi = str(line['PR_PFI'])
        ufi = str(line['UFI'])

        line_values = (parcel_pfi, pr_pfi, ufi)

        cursor.execute("INSERT INTO PARCEL_PROPERTY VALUES ( ?, ?, ?)", line_values)
        line_count += 1

    connection.commit()
    print(f"\nImporting Finished, imported " + str(line_count - 1) + " records into PARCEL_PROPERTY\n")

    try:
        print('Creating INDEX "PARCEL_TO_PROP", please wait...')
        cursor.execute('CREATE INDEX "PARCEL_TO_PROP" ON "PARCEL_PROPERTY" ("PARCEL_PFI" ASC, "PR_PFI" ASC)')
        connection.commit()
        print('Finished creating INDEX "PARCEL_TO_PROP"')
    except Exception as e:
        print(e)
        print('Failed creating INDEX "PARCEL_TO_PROP"', sys.exc_info()[0])

    try:
        print('Creating INDEX "PROP_TO_PARCEL", please wait...')
        cursor.execute('CREATE INDEX "PROP_TO_PARCEL" ON "PARCEL_PROPERTY" ("PR_PFI" ASC, "PARCEL_PFI" ASC)')
        connection.commit()
        print('Finished creating INDEX "PROP_TO_PARCEL"')
    except Exception as e:
        print(e)
        print('Failed creating INDEX "PROP_TO_PARCEL"', sys.exc_info()[0])


def parcel_lot_table():
    # PARCEL LOT TABLE
    print('Beginning PARCEL IMPORT:\n')
    try:
        cursor.execute("DROP TABLE PARCEL_LOT")
        print('Table PARCEL_LOT Dropped')
    except sqlite3.OperationalError:
        cursor.execute("CREATE TABLE PARCEL_LOT (PFI, LGA_CODE, PLAN_NO, LOT_NUMBER)")
        print('Table PARCEL_LOT Created')

    try:
        cursor.execute("CREATE TABLE PARCEL_LOT (PFI, LGA_CODE, PLAN_NO, LOT_NUMBER)")
        print('Table PARCEL_LOT Created')
    except sqlite3.OperationalError:
        pass

    print('Importing data into: PARCEL_LOT, this may take a while please wait...')
    line_count = 1

    for line in records('Shapefiles/PARCEL.dbf'):
        print(f'\r  Records imported: {line_count}', end='', flush=True)
        parcel_pfi = str(line['PFI'])
        lga = str(line['LGA_CODE'])
        plan = str(line['PLAN_NO'])
        lot = str(line['LOT_NUMBER'])

        line_values = (parcel_pfi, lga, plan, lot)

        cursor.execute("INSERT INTO PARCEL_LOT VALUES ( ?, ?, ?, ?)", line_values)
        line_count += 1

    connection.commit()
    print(f"\nImporting Finished, imported " + str(line_count - 1) + " records into PARCEL_LOT\n")

    try:
        print('Creating INDEX "LOT NUMBER", please wait...')
        cursor.execute('CREATE INDEX "LOT NUMBER" ON "PARCEL_LOT" ("PFI" ASC, "LOT_NUMBER" ASC, "LGA_CODE" ASC)')
        connection.commit()
        print('Finished creating INDEX "LOT NUMBER"')
    except Exception as e:
        print(e)
        print('Failed creating INDEX "LOT NUMBER"', sys.exc_info()[0])


def address_table():
    # ADDRESS TABLE
    print('Beginning ADDRESS IMPORT:\n')
    try:
        cursor.execute("DROP TABLE ADDRESS")
        print('Table ADDRESS Dropped')
    except sqlite3.OperationalError:
        cursor.execute("CREATE TABLE ADDRESS (PFI, PR_PFI, EZI_ADD, SOURCE,"
                       "BLGUNTTYP, BUNIT_PRE1, BUNIT_ID1, BUNIT_SUF1, BUNIT_PRE2, BUNIT_ID2, BUNIT_SUF2,"
                       "FLOOR_TYPE, FL_PREF1, FLOOR_NO_1, FL_SUF1, FL_PREF2, FLOOR_NO_2, FL_SUF2,"
                       "BUILDING, COMPLEX, HSE_PREF1, HSE_NUM1, HSE_SUF1, HSE_PREF2, HSE_NUM2, HSE_SUF2,"
                       "DISP_PREF1, DISP_NUM1, DISP_SUF1, DISP_PREF2, DISP_NUM2, DISP_SUF2,"
                       "ROAD_NAME, ROAD_TYPE, RD_SUF, LOCALITY, LGA_CODE,"
                       "STATE, POSTCODE, NUM_RD_ADD, NUM_ADD, 'COORDINATES')")
        print('Table ADDRESS Created')

    try:
        cursor.execute("CREATE TABLE ADDRESS (PFI, PR_PFI, EZI_ADD, SOURCE,"
                       "BLGUNTTYP, BUNIT_PRE1, BUNIT_ID1, BUNIT_SUF1, BUNIT_PRE2, BUNIT_ID2, BUNIT_SUF2,"
                       "FLOOR_TYPE, FL_PREF1, FLOOR_NO_1, FL_SUF1, FL_PREF2, FLOOR_NO_2, FL_SUF2,"
                       "BUILDING, COMPLEX, HSE_PREF1, HSE_NUM1, HSE_SUF1, HSE_PREF2, HSE_NUM2, HSE_SUF2,"
                       "DISP_PREF1, DISP_NUM1, DISP_SUF1, DISP_PREF2, DISP_NUM2, DISP_SUF2,"
                       "ROAD_NAME, ROAD_TYPE, RD_SUF, LOCALITY, LGA_CODE,"
                       "STATE, POSTCODE, NUM_RD_ADD, NUM_ADD, 'COORDINATES')")
        print('Table ADDRESS Created')
    except sqlite3.OperationalError:
        pass

    print('Importing data into: ADDRESS, this may take a while please wait...')

    table_names = ['PFI', 'PR_PFI', 'EZI_ADD', 'SOURCE', 'BLGUNTTYP', 'BUNIT_PRE1', 'BUNIT_ID1', 'BUNIT_SUF1', 'BUNIT_PRE2', 'BUNIT_ID2',
                   'BUNIT_SUF2', 'FLOOR_TYPE', 'FL_PREF1', 'FLOOR_NO_1', 'FL_SUF1', 'FL_PREF2', 'FLOOR_NO_2', 'FL_SUF2', 'BUILDING', 'COMPLEX',
                   'HSE_PREF1', 'HSE_NUM1', 'HSE_SUF1', 'HSE_PREF2', 'HSE_NUM2', 'HSE_SUF2', 'DISP_PREF1', 'DISP_NUM1', 'DISP_SUF1', 'DISP_PREF2',
                   'DISP_NUM2', 'DISP_SUF2', 'ROAD_NAME', 'ROAD_TYPE', 'RD_SUF', 'LOCALITY', 'LGA_CODE', 'STATE', 'POSTCODE', 'NUM_RD_ADD',
                   'NUM_ADD', 'COORDINATES']

    line_count = 1

    # Creates INTO statement with correct amount of ? bindings == to len(table_names)
    binding_count = "INSERT INTO ADDRESS VALUES ( ?"
    for i in range(len(table_names) - 1):
        binding_count = binding_count + ', ?'
    binding_count = binding_count + ')'

    for line in shape_records('Shapefiles/ADDRESS'):
        print(f'\r  Records imported: {line_count}', end='', flush=True)

        line_values = []

        for table in table_names:
            line_values.append(str(line[table]))
        cursor.execute(binding_count, line_values)
        line_count += 1

    # Attempts to import records from second ADDRESS shapefile if it exists
    try:
        for line in shape_records('Shapefiles/ADDRESS_1'):
            print(f'\r  Records imported: {line_count}', end='', flush=True)

            line_values = []

            for table in table_names:
                line_values.append(str(line[table]))

            cursor.execute(binding_count, line_values)
            line_count += 1

    except Exception as e:
        print(e)
        print("ERROR OCCURRED AT ADDRESS_1", sys.exc_info()[0])
        pass

    connection.commit()
    print(f"\nImporting Finished, imported " + str(line_count - 1) + " records into ADDRESS\n")

    try:
        print('Creating INDEX "ADDRESS INDEX", please wait...')
        cursor.execute('CREATE INDEX "ADDRESS INDEX" ON "ADDRESS" ("PR_PFI"	ASC, "HSE_NUM1"	ASC, "ROAD_NAME" ASC, "LOCALITY" ASC, "POSTCODE" ASC)')
        connection.commit()
        print('Finished creating INDEX "ADDRESS INDEX"')
    except Exception as e:
        print(e)
        print('Failed creating INDEX "ADDRESS INDEX"', sys.exc_info()[0])


if __name__ == '__main__':

    print('To being import make sure update files are available, type "Yes" to proceed or "No" to quit.')
    user_input = input().upper()
    if user_input == 'YES':
        print("Select tables to import:\n")
        print('1 : Parcel Property Join Table')
        print('2 : Parcel/Lot Table')
        print('3 : Address Table')
        print('4 : All tables')
        print('Type anything else to exit.')
        user_input = input()
        if user_input == '1':
            parcel_property_join_table()
            connection.close()
            print('Finished all imports, press any key to close program...')
            input()
            quit()
        elif user_input == '2':
            parcel_lot_table()
            connection.close()
            print('Finished all imports, press any key to close program...')
            input()
            quit()
        elif user_input == '3':
            address_table()
            connection.close()
            print('Finished all imports, press any key to close program...')
            input()
            quit()
        elif user_input == '4':
            parcel_property_join_table()
            parcel_lot_table()
            address_table()
            connection.close()
            print('Finished all imports, press any key to close program...')
            input()
            quit()
        else:
            print("Exiting.")
            connection.close()
            input()
            quit()

    if user_input == 'NO':
        connection.close()
        print('Press any key to close program...')
        input()
        quit()
    else:
        print('Command unrecognized, press any key to close program...')
        input()
        quit()

'''

'''


EXAMPLE:

>>> from db_utils import db_connect
>>> con = db_connect() # connect to the database
>>> cur = con.cursor() # instantiate a cursor obj
>>> customers_sql = """
... CREATE TABLE customers (
...     id integer PRIMARY KEY,
...     first_name text NOT NULL,
...     last_name text NOT NULL)"""
>>> cur.execute(customers_sql)
>>> products_sql = """
... CREATE TABLE products (
...     id integer PRIMARY KEY,
...     name text NOT NULL,
...     price real NOT NULL)"""
>>> cur.execute(products_sql)




def testing():
    try:
        cursor.execute("DROP TABLE ADDRESS")
        print('Table ADDRESS Dropped')
    except sqlite3.OperationalError:
        cursor.execute("CREATE TABLE ADDRESS (PFI, PR_PFI, EZI_ADD, SOURCE,"
                       "BLGUNTTYP, BUNIT_PRE1, BUNIT_ID1, BUNIT_SUF1, BUNIT_PRE2, BUNIT_ID2, BUNIT_SUF2,"
                       "FLOOR_TYPE, FL_PREF1, FLOOR_NO_1, FL_SUF1, FL_PREF2, FLOOR_NO_2, FL_SUF2,"
                       "BUILDING, COMPLEX, HSE_PREF1, HSE_NUM1, HSE_SUF1, HSE_PREF2, HSE_NUM2, HSE_SUF2,"
                       "DISP_PREF1, DISP_NUM1, DISP_SUF1, DISP_PREF2, DISP_NUM2, DISP_SUF2,"
                       "ROAD_NAME, ROAD_TYPE, RD_SUF, LOCALITY, LGA_CODE,"
                       "STATE, POSTCODE, NUM_RD_ADD, NUM_ADD)")
        print('Table ADDRESS Created')

    try:
        cursor.execute("CREATE TABLE ADDRESS (PFI, PR_PFI, EZI_ADD, SOURCE,"
                       "BLGUNTTYP, BUNIT_PRE1, BUNIT_ID1, BUNIT_SUF1, BUNIT_PRE2, BUNIT_ID2, BUNIT_SUF2,"
                       "FLOOR_TYPE, FL_PREF1, FLOOR_NO_1, FL_SUF1, FL_PREF2, FLOOR_NO_2, FL_SUF2,"
                       "BUILDING, COMPLEX, HSE_PREF1, HSE_NUM1, HSE_SUF1, HSE_PREF2, HSE_NUM2, HSE_SUF2,"
                       "DISP_PREF1, DISP_NUM1, DISP_SUF1, DISP_PREF2, DISP_NUM2, DISP_SUF2,"
                       "ROAD_NAME, ROAD_TYPE, RD_SUF, LOCALITY, LGA_CODE,"
                       "STATE, POSTCODE, NUM_RD_ADD, NUM_ADD)")
        print('Table ADDRESS Created')
    except sqlite3.OperationalError:
        pass

    for line in shape_records('ADDRESS'):
        print(line)
'''