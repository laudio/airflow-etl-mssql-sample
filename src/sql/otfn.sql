SELECT
    t.employee_id,
    t.date,
	(CASE
		WHEN (CONVERT (time, CURRENT_TIMESTAMP) between punch_in_time and punch_out_time) THEN 1
		WHEN (CONVERT (time, CURRENT_TIMESTAMP) > punch_in_time and punch_out_time IS NULL) THEN 1
		else 0
	END) as is_otfn
FROM timesheet t;
