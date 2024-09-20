import requests
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def extract_data(url):
    response = requests.get(url)
    return response.json()

def create_table(cursor):
    try:
        # Create schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.rdw_voertuigen (
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
        """
        cursor.execute(create_table_sql)
        print("RDW voertuigen table created successfully")
    except Exception as e:
        print(f"Error creating RDW voertuigen table: {e}")
        raise

def load_data(data, conn):
    cursor = conn.cursor()
    create_table(cursor)  # Call the create_table function
    
    for record in data:
        columns = ', '.join(record.keys())
        placeholders = ', '.join(['%s'] * len(record))
        values = list(record.values())
        
        cursor.execute(f"""
        INSERT INTO raw.rdw_voertuigen ({columns})
        VALUES ({placeholders})
        ON CONFLICT (kenteken) DO UPDATE SET
        {', '.join([f"{k} = EXCLUDED.{k}" for k in record.keys() if k != 'kenteken'])}
        """, values)

    conn.commit()
    cursor.close()

def main():
    url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
    data = extract_data(url)
    
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    load_data(data, conn)
    conn.close()

if __name__ == "__main__":
    main()