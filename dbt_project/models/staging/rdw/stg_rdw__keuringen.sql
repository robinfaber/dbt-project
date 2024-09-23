with

source as (

    select * from {{ source('rdw','rdw__keuringen') }}

),

renamed as (

    select
        kenteken as voertuig_id,
        to_date(CAST(meld_datum_door_keuringsinstantie AS TEXT), 'YYYYMMDD') as datum_keuring,
        soort_erkenning_omschrijving as soort_erkenning,
        soort_melding_ki_omschrijving as soort_melding,
        case
            when vervaldatum_keuring = 0 then null
            else to_date(CAST(vervaldatum_keuring AS TEXT), 'YYYYMMDD')
        end as vervaldatum_apk
    from source
)

select * from renamed