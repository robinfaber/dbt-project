with

geconstateerde_gebreken as (

    select * from {{ ref('stg_rdw__geconstateerde_gebreken') }}

),

gebreken_beschrijving as (

    select * from {{ ref('stg_rdw__gebreken_beschrijving') }}

),

count_gebreken as (

    select 
        gebrek_id,
        count(voertuig_id) as aantal_voertuigen
    from geconstateerde_gebreken
    group by gebrek_id
)

select

    g.gebrek_id,
    g.gebrek_omschrijving,
    g.ingangsdatum_gebrek,
    c.aantal_voertuigen

from gebreken_beschrijving g
left join count_gebreken c
    on g.gebrek_id = c.gebrek_id
where g.einddatum_gebrek is null
