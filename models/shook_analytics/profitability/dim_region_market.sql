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
            end as market
        from departments_parsed
    )
select *
from departments_final