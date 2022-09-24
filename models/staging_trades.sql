drop table if exists {{ target_table }};

create table {{ target_table }} as 
    select 
        i as id,
        to_timestamp(t, 'YYYY-MM-DDTHH:MI:SS') as timestamp,
        x as exchange,
        p as trade_price,
        s as trade_size,
        c as trade_conditions,
        z as tape 
    from public.raw_trades 
