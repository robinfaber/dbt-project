with

fact_keuringen as  (

    select  unique_id,
            voertuig_id,
            datum_keuring,
            gebrek_id,
            soort_erkenning,
            soort_melding,
            vervaldatum_apk,
            (current_date - vervaldatum_apk) as days_to_apk_verval,
             case when (vervaldatum_apk - current_date) < 0 then 'expired'
                 when (vervaldatum_apk - current_date) <= 7 then 'danger'
                 when (vervaldatum_apk - current_date) <= 30 then 'warning'
                 else 'safe'
            end as apk_verval_status
    from {{ ref('int_keuringen_gebreken_joined' )}}
)

select * from fact_keuringen
