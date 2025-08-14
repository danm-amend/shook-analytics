select 
    * 
from 
    {{ source('sharepoint', 'rolling_forecast') }}