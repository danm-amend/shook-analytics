with practice_areas as (
    select 
        assessmentpercentage as assessment_percentage
        , available
        , cast(createdate as timestamp) as create_date
        , deleterecord as delete_record
        , marginpercentage as margin_percentage
        , modifydate as modify_date
        , practiceareaacronym as practice_area_acronym
        , practiceareaaddress1 as practice_area_address_1
        , practiceareaaddress2 as practice_area_address_2
        , practiceareacity as practice_area_city
        , practiceareacountryid as practice_area_country_id
        , practiceareaemail as practice_area_email
        , practiceareafax as practice_area_fax
        , practiceareaid as practice_area_id
        , practiceareaname as practice_area_name
        , practiceareaphone as practice_area_phone
        , practiceareastateid as practice_area_state_id
        , practiceareaurl as practice_area_url
        , practiceareazip as practice_area_zip
        , row_version 
        , userid as user_id
        , old_practiceareaid as old_practice_area_id
        , practiceareasalesgoal as practice_area_sales_goal 
    from 
        {{ source('unanet', 'practiceareas') }}
)

select * from practice_areas