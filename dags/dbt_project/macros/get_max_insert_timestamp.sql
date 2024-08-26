-- https://docs.getdbt.com/docs/build/incremental-models
{% macro get_max_insert_timestamp(date_column) %}
	{% if execute %}
		{% set query %}
	    SELECT
	    		COALESCE(
	    			-- latest timestamp for given timestamp col on table
	    			MAX(EXTRACT(EPOCH from {{ date_column }})::integer),
	    			-- unix epoch (insert * by default)
	    			1522434600
	    		)
	    FROM {{ this }}
		{% endset %}

		{% set max_event_time = run_query(query).columns[0][0] %}

		{% do return(max_event_time) %}
	{% endif %}
{% endmacro %}