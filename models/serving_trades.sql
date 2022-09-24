{# perform an incremental load #}

{# check if the table already exists #}
{% set table_exists = engine.execute(
    "select exists (
        select tablename 
        from pg_tables 
        where tablename='" + target_table + "')"
).first()[0]%}

{# if the table exists then get the most recent record #}
{% if table_exists %}

    insert into {{ target_table }}

{% else %}

    create table {{ target_table }} as 

{% endif %}
(
    select 
        id,
        timestamp,
        exchange_name,
        trade_price,
        trade_size,
        trade_conditions,
        tape 
    from staging_trades st 
    inner join tbl_exchange_codes sec 
        on st.exchange = sec.exchange_code 

    {% if table_exists %}

        {% set max_timestamp = engine.execute("select max(timestamp)::timestamp from " + target_table).first()[0] %}
        where timestamp > '{{ max_timestamp }}'

    {% endif %}
);
