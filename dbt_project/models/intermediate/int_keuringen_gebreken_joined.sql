with

keuringen as  (

    select * from {{ ref('stg_rdw__keuringen' )}}

),

geconstateerde_gebreken as (

    select * from {{ ref('stg_rdw__geconstateerde_gebreken') }}

),

keuringen_gebreken_joined as (

    select 
            md5(k.voertuig_id || k.datum_keuring || gg.gebrek_id) as unique_id,
            k.voertuig_id,
            k.datum_keuring,
            gg.gebrek_id,
            k.soort_erkenning,
            k.soort_melding,
            k.vervaldatum_apk
    from keuringen k
    left join geconstateerde_gebreken gg
        on k.voertuig_id = gg.voertuig_id

)

select * from keuringen_gebreken_joined