-- https://docs.getdbt.com/docs/build/incremental-models
{% macro get_max_insert_date_string(date_column) %}
	{% if execute %}
		{% set query %}
	    SELECT
	    		COALESCE(
	    			-- latest timestamp for given timestamp col on table
	    			MAX({{ date_column }}),
	    			-- unix epoch (insert * by default)
	    			'1970-01-01'
	    		)
	    FROM {{ this }}
		{% endset %}

		{% set max_event_time = run_query(query).columns[0][0] %}

		{% do return(max_event_time) %}
	{% endif %}
{% endmacro %}