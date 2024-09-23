import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def create_voertuigen_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.rdw__voertuigen CASCADE;
        CREATE TABLE IF NOT EXISTS raw.rdw__voertuigen (
            kenteken VARCHAR(255) PRIMARY KEY,
            voertuigsoort VARCHAR(255),
            merk VARCHAR(255),
            handelsbenaming VARCHAR(255),
            vervaldatum_apk DATE,
            datum_tenaamstelling DATE,
            bruto_bpm NUMERIC,
            inrichting VARCHAR(255),
            aantal_zitplaatsen INTEGER,
            eerste_kleur VARCHAR(255),
            tweede_kleur VARCHAR(255),
            aantal_cilinders INTEGER,
            cilinderinhoud INTEGER,
            massa_ledig_voertuig INTEGER,
            toegestane_maximum_massa_voertuig INTEGER,
            massa_rijklaar INTEGER,
            maximum_massa_trekken_ongeremd INTEGER,
            maximum_trekken_massa_geremd INTEGER,
            datum_eerste_toelating DATE,
            datum_eerste_tenaamstelling_in_nederland DATE,
            wacht_op_keuren VARCHAR(255),
            catalogusprijs NUMERIC,
            wam_verzekerd VARCHAR(255),
            maximale_constructiesnelheid INTEGER,
            laadvermogen NUMERIC,
            oplegger_geremd VARCHAR(255),
            aanhangwagen_autonoom_geremd VARCHAR(255),
            aanhangwagen_middenas_geremd VARCHAR(255),
            aantal_staanplaatsen INTEGER,
            aantal_deuren INTEGER,
            aantal_wielen INTEGER,
            afstand_hart_koppeling_tot_achterzijde_voertuig INTEGER,
            afstand_voorzijde_voertuig_tot_hart_koppeling INTEGER,
            afwijkende_maximum_snelheid NUMERIC,
            lengte INTEGER,
            breedte INTEGER,
            europese_voertuigcategorie VARCHAR(255),
            europese_voertuigcategorie_toevoeging VARCHAR(255),
            europese_uitvoeringcategorie_toevoeging VARCHAR(255),
            plaats_chassisnummer VARCHAR(255),
            technische_max_massa_voertuig INTEGER,
            type VARCHAR(255),
            type_gasinstallatie VARCHAR(255),
            typegoedkeuringsnummer VARCHAR(255),
            variant VARCHAR(255),
            uitvoering VARCHAR(255),
            volgnummer_wijziging_eu_typegoedkeuring INTEGER,
            vermogen_massarijklaar NUMERIC,
            wielbasis INTEGER,
            export_indicator VARCHAR(255),
            openstaande_terugroepactie_indicator VARCHAR(255),
            vervaldatum_tachograaf DATE,
            taxi_indicator VARCHAR(255),
            maximum_massa_samenstelling INTEGER,
            aantal_rolstoelplaatsen INTEGER,
            maximum_ondersteunende_snelheid NUMERIC,
            jaar_laatste_registratie_tellerstand INTEGER,
            tellerstandoordeel VARCHAR(255),
            code_toelichting_tellerstandoordeel VARCHAR(255),
            tenaamstellen_mogelijk VARCHAR(255),
            vervaldatum_apk_dt TIMESTAMP,
            datum_tenaamstelling_dt TIMESTAMP,
            datum_eerste_toelating_dt TIMESTAMP,
            datum_eerste_tenaamstelling_in_nederland_dt TIMESTAMP,
            vervaldatum_tachograaf_dt TIMESTAMP,
            maximum_last_onder_de_vooras_sen_tezamen_koppeling NUMERIC,
            type_remsysteem_voertuig_code VARCHAR(255),
            rupsonderstelconfiguratiecode VARCHAR(255),
            wielbasis_voertuig_minimum INTEGER,
            wielbasis_voertuig_maximum INTEGER,
            lengte_voertuig_minimum INTEGER,
            lengte_voertuig_maximum INTEGER,
            breedte_voertuig_minimum INTEGER,
            breedte_voertuig_maximum INTEGER,
            hoogte_voertuig INTEGER,
            hoogte_voertuig_minimum INTEGER,
            hoogte_voertuig_maximum INTEGER,
            massa_bedrijfsklaar_minimaal INTEGER,
            massa_bedrijfsklaar_maximaal INTEGER,
            technisch_toelaatbaar_massa_koppelpunt INTEGER,
            maximum_massa_technisch_maximaal INTEGER,
            maximum_massa_technisch_minimaal INTEGER,
            subcategorie_nederland VARCHAR(255),
            verticale_belasting_koppelpunt_getrokken_voertuig NUMERIC,
            zuinigheidsclassificatie VARCHAR(255),
            registratie_datum_goedkeuring_afschrijvingsmoment_bpm DATE,
            registratie_datum_goedkeuring_afschrijvingsmoment_bpm_dt TIMESTAMP,
            gem_lading_wrde NUMERIC,
            aerodyn_voorz VARCHAR(255),
            massa_alt_aandr NUMERIC,
            verl_cab_ind VARCHAR(255),
            api_gekentekende_voertuigen_assen VARCHAR(255),
            api_gekentekende_voertuigen_brandstof VARCHAR(255),
            api_gekentekende_voertuigen_carrosserie VARCHAR(255),
            api_gekentekende_voertuigen_carrosserie_specifiek VARCHAR(255),
            api_gekentekende_voertuigen_voertuigklasse VARCHAR(255)
        )
        """)
        print("Table voertuigen created successfully")
    except Exception as e:
        print(f"Error creating table voertuigen: {e}")
        raise

def create_keuringen_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.rdw__keuringen CASCADE;
        CREATE TABLE IF NOT EXISTS raw.rdw__keuringen (
            kenteken VARCHAR(255),
            soort_erkenning_keuringsinstantie VARCHAR(255),
            meld_datum_door_keuringsinstantie INTEGER,
            meld_tijd_door_keuringsinstantie INTEGER,
            soort_erkenning_omschrijving TEXT,
            soort_melding_ki_omschrijving TEXT,
            vervaldatum_keuring INTEGER,
            meld_datum_door_keuringsinstantie_dt TIMESTAMP,
            vervaldatum_keuring_dt TIMESTAMP,
            api_gebrek_constateringen TEXT,
            api_gebrek_beschrijving TEXT,
            PRIMARY KEY(kenteken, meld_datum_door_keuringsinstantie)
        )
        """)
        print("Table rdw__keuringen created successfully")
    except Exception as e:
        print(f"Error creating table rdw__keuringen: {e}")
        raise

def create_geconstateerde_gebreken_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.rdw__geconstateerde_gebreken CASCADE;
        CREATE TABLE IF NOT EXISTS raw.rdw__geconstateerde_gebreken (
            kenteken VARCHAR(255),
            soort_erkenning_keuringsinstantie VARCHAR(255),
            meld_datum_door_keuringsinstantie INTEGER,
            meld_tijd_door_keuringsinstantie INTEGER,
            gebrek_identificatie VARCHAR(255),
            soort_erkenning_omschrijving TEXT,
            aantal_gebreken_geconstateerd INTEGER,
            meld_datum_door_keuringsinstantie_dt TIMESTAMP,
            meld_datum_computed INTEGER,
            PRIMARY KEY(kenteken, meld_datum_door_keuringsinstantie, meld_tijd_door_keuringsinstantie, gebrek_identificatie)
        )
        """)
        print("Table rdw__geconstateerde_gebreken created successfully")
    except Exception as e:
        print(f"Error creating table rdw__geconstateerde_gebreken: {e}")
        raise

def create_gebreken_beschrijving_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.rdw__gebreken_beschrijving CASCADE;
        CREATE TABLE IF NOT EXISTS raw.rdw__gebreken_beschrijving (
            gebrek_identificatie VARCHAR(255) PRIMARY KEY,
            ingangsdatum_gebrek INTEGER,
            einddatum_gebrek INTEGER,
            gebrek_paragraaf_nummer INTEGER,
            gebrek_artikel_nummer VARCHAR(255),
            gebrek_omschrijving TEXT,
            ingangsdatum_gebrek_dt TIMESTAMP,
            einddatum_gebrek_dt TIMESTAMP,
            ingangsdatum_gebrek_computed INTEGER,
            einddatum_gebrek_computed INTEGER
        )
        """)
        print("Table rdw__gebreken_beschrijving created successfully")
    except Exception as e:
        print(f"Error creating table rdw__gebreken_beschrijving: {e}")
        raise

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from {url}")

def load_data(cursor, table_name, data):
    if not data:
        print(f"No data to insert for table {table_name}")
        return

    # Get the column names from the first record
    columns = list(data[0].keys())
    
    # Prepare the SQL statement
    columns_sql = ', '.join(columns)
    values_sql = ', '.join(['%s'] * len(columns))
    
    if table_name == 'rdw__geconstateerde_gebreken':
        conflict_columns = 'kenteken, meld_datum_door_keuringsinstantie, meld_tijd_door_keuringsinstantie, gebrek_identificatie'
    elif table_name == 'rdw__gebreken_beschrijving':
        conflict_columns = 'gebrek_identificatie'
    elif table_name == 'rdw__keuringen':
        conflict_columns = 'kenteken, meld_datum_door_keuringsinstantie'
    else:
        conflict_columns = 'kenteken'
    
    update_sql = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns.split(', ')])
    
    insert_sql = f"""
    INSERT INTO raw.{table_name} ({columns_sql})
    VALUES ({values_sql})
    ON CONFLICT ({conflict_columns}) DO UPDATE SET
    {update_sql}
    """
    
    print(f"SQL for {table_name}:")
    print(insert_sql)
    
    # Prepare the data
    values = [[record.get(column) for column in columns] for record in data]
    
    try:
        cursor.executemany(insert_sql, values)
        print(f"Inserted/Updated {len(values)} records into {table_name}")
    except Exception as e:
        print(f"Error inserting records into {table_name}: {e}")
        raise

def main():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    
    cursor = conn.cursor()

    try:
        # Create schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Create tables
        create_voertuigen_table(cursor)
        create_keuringen_table(cursor)
        create_geconstateerde_gebreken_table(cursor)
        create_gebreken_beschrijving_table(cursor)
        # Define tables and their corresponding URLs
        tables = [
            ("rdw__voertuigen", "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$where=starts_with(kenteken, '44')&$limit=10000"),
            ("rdw__keuringen", "https://opendata.rdw.nl/resource/sgfe-77wx.json?$where=starts_with(kenteken, '44')&$limit=10000"),
            ("rdw__geconstateerde_gebreken", "https://opendata.rdw.nl/resource/a34c-vvps.json?$where=starts_with(kenteken, '44')&$limit=10000"),
            ("rdw__gebreken_beschrijving", "https://opendata.rdw.nl/resource/hx2c-gt7k.json")
        ]

        # Fetch and load data for each table
        for table_name, url in tables:
            print(f"Fetching data for {table_name}...")
            data = fetch_data(url)
            print(f"Loading data into {table_name}...")
            load_data(cursor, table_name, data)

        conn.commit()
        print("Data loaded successfully")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()