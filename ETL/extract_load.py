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
        DROP TABLE IF EXISTS raw.voertuigen;
        CREATE TABLE IF NOT EXISTS raw.voertuigen (
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

def create_voertuigen_brandstof_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.voertuigen_brandstof;
        CREATE TABLE IF NOT EXISTS raw.voertuigen_brandstof (
            kenteken VARCHAR(255) PRIMARY KEY,
            brandstof_volgnummer VARCHAR(255),
            brandstof_omschrijving VARCHAR(255),
            brandstofverbruik_buiten NUMERIC,
            brandstofverbruik_gecombineerd NUMERIC,
            brandstofverbruik_stad NUMERIC,
            co2_uitstoot_gecombineerd INTEGER,
            co2_uitstoot_gewogen INTEGER,
            geluidsniveau_rijdend INTEGER,
            geluidsniveau_stationair INTEGER,
            emissiecode_omschrijving VARCHAR(255),
            milieuklasse_eg_goedkeuring_licht VARCHAR(255),
            milieuklasse_eg_goedkeuring_zwaar VARCHAR(255),
            uitstoot_deeltjes_licht NUMERIC,
            uitstoot_deeltjes_zwaar NUMERIC,
            nettomaximumvermogen INTEGER,
            nominaal_continu_maximumvermogen INTEGER,
            roetuitstoot NUMERIC,
            toerental_geluidsniveau INTEGER,
            emis_deeltjes_type1_wltp NUMERIC,
            emissie_co2_gecombineerd_wltp INTEGER,
            emis_co2_gewogen_gecombineerd_wltp INTEGER,
            brandstof_verbruik_gecombineerd_wltp NUMERIC,
            brandstof_verbruik_gewogen_gecombineerd_wltp NUMERIC,
            elektrisch_verbruik_enkel_elektrisch_wltp NUMERIC,
            actie_radius_enkel_elektrisch_wltp INTEGER,
            actie_radius_enkel_elektrisch_stad_wltp INTEGER,
            elektrisch_verbruik_extern_opladen_wltp NUMERIC,
            actie_radius_extern_opladen_wltp INTEGER,
            actie_radius_extern_opladen_stad_wltp INTEGER,
            max_vermogen_15_minuten INTEGER,
            max_vermogen_60_minuten INTEGER,
            netto_max_vermogen_elektrisch INTEGER,
            klasse_hybride_elektrisch_voertuig VARCHAR(255),
            opgegeven_maximum_snelheid INTEGER,
            uitlaatemissieniveau VARCHAR(255)
        )
        """)
        print("Table voertuigen_brandstof created successfully")
    except Exception as e:
        print(f"Error creating table voertuigen_brandstof: {e}")
        raise

def create_voertuigen_carrosserie_specificatie_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.voertuigen_carrosserie_specificatie;
        CREATE TABLE IF NOT EXISTS raw.voertuigen_carrosserie_specificatie (
            kenteken VARCHAR(255) PRIMARY KEY,
            carrosserie_volgnummer VARCHAR(255),
            carrosserie_voertuig_nummer_code_volgnummer VARCHAR(255),
            carrosseriecode VARCHAR(255),
            carrosserie_voertuig_nummer_europese_omschrijving VARCHAR(255)
        )
        """)
        print("Table voertuigen_carrosserie_specificatie created successfully")
    except Exception as e:
        print(f"Error creating table voertuigen_carrosserie_specificatie: {e}")
        raise

def create_voertuigen_assen_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.voertuigen_assen;
        CREATE TABLE IF NOT EXISTS raw.voertuigen_assen (
            kenteken VARCHAR(255) PRIMARY KEY,
            as_nummer INTEGER,
            aantal_assen INTEGER,
            aangedreven_as VARCHAR(255),
            hefas VARCHAR(255),
            plaatscode_as VARCHAR(255),
            spoorbreedte INTEGER,
            weggedrag_code VARCHAR(255),
            wettelijk_toegestane_maximum_aslast INTEGER,
            technisch_toegestane_maximum_aslast INTEGER,
            geremde_as_indicator VARCHAR(255),
            afstand_tot_volgende_as_voertuig INTEGER,
            afstand_tot_volgende_as_voertuig_minimum INTEGER,
            afstand_tot_volgende_as_voertuig_maximum INTEGER,
            maximum_last_as_technisch_maximum INTEGER,
            maximum_last_as_technisch_minimum INTEGER
        )
        """)
        print("Table voertuigen_assen created successfully")
    except Exception as e:
        print(f"Error creating table voertuigen_assen: {e}")
        raise

def create_voertuigen_voertuigklasse_table(cursor):
    try:
        cursor.execute("""
        DROP TABLE IF EXISTS raw.voertuigen_voertuigklasse;
        CREATE TABLE IF NOT EXISTS raw.voertuigen_voertuigklasse (
            kenteken VARCHAR(255) PRIMARY KEY,
            carrosserie_volgnummer VARCHAR(255),
            carrosserie_klasse_volgnummer VARCHAR(255),
            voertuigklasse VARCHAR(255),
            voertuigklasse_omschrijving VARCHAR(255)
        )
        """)
        print("Table voertuigen_voertuigklasse created successfully")
    except Exception as e:
        print(f"Error creating table voertuigen_voertuigklasse: {e}")
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
    update_sql = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'kenteken'])
    
    insert_sql = f"""
    INSERT INTO raw.{table_name} ({columns_sql})
    VALUES ({values_sql})
    ON CONFLICT (kenteken) DO UPDATE SET
    {update_sql}
    """
    
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
        create_voertuigen_brandstof_table(cursor)
        create_voertuigen_carrosserie_specificatie_table(cursor)
        create_voertuigen_assen_table(cursor)
        create_voertuigen_voertuigklasse_table(cursor)

        # Define tables and their corresponding URLs
        tables = [
            ("voertuigen", "https://opendata.rdw.nl/resource/m9d7-ebf2.json"),
            ("voertuigen_brandstof", "https://opendata.rdw.nl/resource/8ys7-d773.json"),
            ("voertuigen_carrosserie_specificatie", "https://opendata.rdw.nl/resource/jhie-znh9.json"),
            ("voertuigen_assen", "https://opendata.rdw.nl/resource/3huj-srit.json"),
            ("voertuigen_voertuigklasse", "https://opendata.rdw.nl/resource/kmfi-hrps.json")
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