with

source as (

    select * from {{ source('rdw','rdw__geconstateerde_gebreken') }}

),

renamed as (

    select
        
        kenteken as voertuig_id,
        to_date(CAST(meld_datum_door_keuringsinstantie AS TEXT), 'YYYYMMDD') as datum_keuring,
        gebrek_identificatie as gebrek_id
    from source
)

select * from renamed