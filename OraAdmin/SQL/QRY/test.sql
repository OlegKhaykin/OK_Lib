SELECT
  t.project,
  t.project_name,
  t.status,
  s.status_name,
  pc.employee_name pc_name,
  se.employee_name se_name,
  sse.employee_name sse_name,
  t.wo_type,
  t.site_address,
  t.site,
  t.wo_number,
  t.source,
  t.bucket,
  t.bucket_name,
  t.division,
  ed.department_name division_name,
  t.wo_cost_center,
  t.cost_center,
  t.last_update,
  t.employee_id,
  e.employee_name,
  t.company_code,
  t.description,
  t.regular_hours,
  t.overtime_hours,
  t.double_time_hours,
  t.travel_hours,
  t.rate_type,
  t.cost,
  t.revenue
FROM
  REL1.VW_DTL_ALL_TRAN_2 t,
rel1.status s,
(
  SELECT
    employee_name, jde_id
  FROM rel1.jde_users
  WHERE role='RT'
) pc,
(
  SELECT
    employee_name, jde_id
  FROM rel1.jde_users
  WHERE role='RT'
) se,
(
  SELECT employee_name, jde_id
  FROM rel1.jde_users
  WHERE role='RT'
) sse,
rel1.estimate_department ed,
rel1.employee e
WHERE t.status = s.status(+)
and t.pc = pc.jde_id(+)
and t.se = se.jde_id(+)
and t.sse = sse.jde_id(+)
and t.division = ed.estimate_department
and t.employee_id = e.ssn(+)
and t.project= 176975
and t.fiscal_date between '01-apr-2002' and '18-apr-2002'
order by bucket,division,
last_UPDATE desc
