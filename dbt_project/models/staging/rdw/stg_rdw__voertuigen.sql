with

source as (

    select * from {{ source('rdw','rdw__voertuigen') }}

),

renamed as (

    select
        kenteken as voertuig_id,

        -- case 
        --     when voertuigsoort in ('Personenauto', 'Bedrijfsauto') then 'Auto'
        --     when voertuigsoort in ('Motorfiets', 'Driewielig motorrijtuig') then 'Motor'
        --     else 'Overige'
        -- end as hoofdtype_voertuig,

        {% set voertuigsoort_mapping = {
            'Personenauto': 'Auto',
            'Bedrijfsauto': 'Auto',
            'Motorfiets': 'Motor',
            'Driewielig motorrijtuig': 'Motor'
        } %}

        case 
            {% for key, value in voertuigsoort_mapping.items() %}
                when voertuigsoort = '{{ key }}' then '{{ value }}'
            {% endfor %}
            else 'Overige'
        end as hoofdtype_voertuig,

        voertuigsoort as subtype_voertuig,
        inrichting as type_carrosserie,
        merk,
        handelsbenaming as model,
        eerste_kleur as kleur,
        aantal_zitplaatsen,
        aantal_deuren,
        aantal_wielen,
        datum_eerste_toelating,
        datum_tenaamstelling,
        datum_eerste_tenaamstelling_in_nederland as datum_eerste_tenaamstelling,
        vervaldatum_apk,
        tellerstandoordeel,
        catalogusprijs as prijs,

        case
            when wam_verzekerd = 'Ja' then true
            else false
        end as is_wam_verzekerd

    from source

)

select * from renamed