with
    departments_base as (
        select
            *
            ,replace(description, 'Region - ', '') as sub_desc
        from
            shookdw.viewpoint.bjcdm
        where 
            description like 'Region%'
    ),
    departments_parsed as (
        select
            *
            ,case
                when sub_desc like 'Great Lakes%' then 'Great Lakes'
                else split(sub_desc, ' ')[0]::string
            end as region
            ,case
                when sub_desc like 'Great Lakes%' then split(sub_desc, ' ')[2]::string
                else split(sub_desc, ' ')[1]::string
            end as market_abbr
            --split(replace(description, 'Region - ', ''), ' ')[1] as market
        from
            departments_base
    ),
    departments_final as (
        select
            department
            ,substring(department, 1, 2) as region_number
            ,substring(department, 3, 4) as market_number
            ,description
            ,region
            , case
                when market_abbr like 'ED%' then 'EDU'
                else market_abbr
            end as market_abbr
            ,case
                when market_abbr like 'ED%' then 'Education'
                when market_abbr like 'HC%' then 'Healthcare'
                when market_abbr like 'IND%' then 'Industrial'
                when market_abbr like 'Water' then 'Water'
            end as market,
            case 
                when market_abbr like 'ED%' then 'Building'
                when market_abbr like 'HC%' then 'Building'
                when market_abbr like 'IND%' then 'Building'
                when market_abbr like 'Water' then 'Water'
            end as market_abstraction
        from departments_parsed
    ), unanet as (
        select 
            *
        from 
            {{ ref('dim_unanet_region_market') }}
    ), unanet_region as (
        select
            distinct 
            office_division_id, 
            office_division_description
        from 
            unanet
    ), unanet_market as (
        select 
            distinct 
            practice_area_id, 
            practice_area_name
        from 
            unanet
    ), unanet_ids as (
        select 
            * 
        from 
            departments_final as a 
        left join 
            unanet_region as b 
        on a.region = b.office_division_description 
        left join 
            unanet_market as c 
        on trim(a.market) = trim(c.practice_area_name)
    )

select 
    department,
    region_number,
    market_number,
    office_division_id as unanet_region_id,
    practice_area_id as unanet_market_id,
    description,
    region,
    market_abbr,
    market,
    market_abstraction
from 
    unanet_ids