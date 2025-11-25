with events as (
    select
        eventid,
        venueid,
        eventdate,
        eventname,
        venuename,
        pricelevel,
        section,
        rownumber,
        seat
    from {{ ref('silver__dim_event') }}
),

transactions as (
    select
        listingid,
        orderdate,
        eventid,
        eventdate,
        section,
        rownumber,
        startseat,
        endseat,
        quantity,
        price,
        posstatus
    from {{ ref('bronze__vw_inventory') }}
),

final as (
    select
        t.listingid,
        t.eventdate,
        t.eventid,
        t.section,
        t.rownumber,
        t.startseat,
        t.endseat,
        t.quantity,
        t.price,
        t.posstatus,
        coalesce(e.eventname, 'Unknown event') as event_name,
        coalesce(e.venueid, null) as venue_id,
        coalesce(e.venuename, 'Unknown venue name') as venue_name,
        coalesce(e.pricelevel, null) as price_level,
        coalesce(e.seat, null) as seat,
        t.quantity * t.price as revenue

    from transactions as t
    left join events as e
        on
            t.eventid = e.eventid
            and t.eventdate = e.eventdate
            and t.section = e.section
            and t.rownumber = e.rownumber

    where t.posstatus = 'Sold'
)

select * from final
