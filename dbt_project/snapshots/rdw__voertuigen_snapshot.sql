{% snapshot rdw__voertuigen_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='kenteken',
      strategy='timestamp',
      updated_at='datum_tenaamstelling'
    )
}}

select * from {{ source('rdw','rdw__voertuigen') }}

{% endsnapshot %}