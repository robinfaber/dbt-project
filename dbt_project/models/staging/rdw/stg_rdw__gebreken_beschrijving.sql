with

source as (

    select * from {{ source('rdw','rdw__gebreken_beschrijving') }}

),

renamed as (

    select
        gebrek_identificatie as gebrek_id,
        gebrek_omschrijving as gebrek_omschrijving,
        to_date(CAST(ingangsdatum_gebrek AS TEXT), 'YYYYMMDD') as ingangsdatum_gebrek,
        to_date(CAST(einddatum_gebrek AS TEXT), 'YYYYMMDD') as einddatum_gebrek
    from source
)

select * from renamed
