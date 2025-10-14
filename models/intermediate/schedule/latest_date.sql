select MAX(start_week) as START_WEEK
from {{ source('P6', 'TASK') }}
