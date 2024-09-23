with

keuringen as  (

    select * from {{ ref('stg_rdw__keuringen' )}}

),

geconstateerde_gebreken as (

    select * from {{ ref('stg_rdw__geconstateerde_gebreken') }}

),

keuringen_gebreken_joined as (

    select 
            k.voertuig_id,
            k.datum_keuring,
            COALESCE(gg.gebrek_id, 'geen_gebrek') as gebrek_id,
            k.soort_erkenning,
            k.soort_melding,
            k.vervaldatum_apk
    from keuringen k
    left join geconstateerde_gebreken gg
        on k.voertuig_id = gg.voertuig_id
        and k.datum_keuring = gg.datum_keuring
        

)

select 
        voertuig_id || datum_keuring::varchar || gebrek_id as unique_id,
        voertuig_id,
        datum_keuring,
        gebrek_id,
        soort_erkenning,
        soort_melding,
        vervaldatum_apk
from keuringen_gebreken_joined