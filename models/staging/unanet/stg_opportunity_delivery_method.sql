with delivery_methods as (
    select 
        "OpportunityId" as opportunity_id,
        "DeliveryMethodID" as delivery_method_id,
        "DeliveryMethodName" as delivery_method_name,
        last_load_dt 
    from 
        {{ source('unanet', 'opportunity_delivery_method') }}
)

select * from delivery_methods