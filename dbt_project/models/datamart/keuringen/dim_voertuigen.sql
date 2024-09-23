with

dim_voertuigen as  (

    select 
    
        voertuig_id,
        hoofdtype_voertuig,
        subtype_voertuig,
        type_carrosserie,
        merk,
        model,
        kleur,
        aantal_zitplaatsen,
        aantal_deuren,
        aantal_wielen,
        datum_eerste_toelating,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, datum_eerste_toelating)) as leeftijd_voertuig,
        datum_tenaamstelling,
        datum_eerste_tenaamstelling,
        vervaldatum_apk,
        tellerstandoordeel,
        prijs,
        is_wam_verzekerd        
    
    from {{ ref('stg_rdw__voertuigen') }}

)

select * from dim_voertuigen