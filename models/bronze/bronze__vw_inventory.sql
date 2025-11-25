select
    listingid,
    orderdate::date as orderdate,
    eventid,
    eventdate::date as eventdate,
    section,
    "Row" as rownumber,
    startseat,
    endseat,
    quantity,
    price,
    posstatus
from {{ source('ticketing_data', 'inventory') }}
