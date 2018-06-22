CREATE TABLE otfn.timesheet
(
    employee_id INT NOT NULL,
    [date] date NOT NULL,
    punch_in_time time,
    punch_out_time time
);
