with region_market as (
    select distinct 
    office_division_id, office_division_description,
    PracticeAreaId as practice_area_id, PracticeAreaName as practice_area_name
    from {{ ref('stg_office_division') }}
    cross join 
    {{ ref('stg_practice_areas') }}
    order by office_division_id, practice_area_id
)

select * from region_market