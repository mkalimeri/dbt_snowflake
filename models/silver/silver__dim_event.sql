with events as (
    select *
    from {{ ref('event') }}
),

venues as (
    select
        id,
        venuename,
        pricelevel,
        section,
        rownumber,
        seat
    from {{ ref('venue') }}
),

final as (
    select
        events.eventid,
        events.venueid,
        events.eventdate,
        events.eventname,
        venues.venuename,
        venues.pricelevel,
        venues.section,
        venues.rownumber,
        venues.seat
    from events
    left join venues
        on events.venueid = venues.id
)

select * from final
