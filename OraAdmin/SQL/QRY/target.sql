SELECT Max(profit) FROM
(
  SELECT /* ORDERED*/
    d.project project,
    NVL(d.closed_date, t.fiscal_date) fiscal_date,
    t.revenue revenue,
    t.revenue * p.percent/100 profit
  FROM
    work_order c,
    jde_transaction t,
    project_profitability p,
    work_order d
  WHERE
    c.wo_type = 'SC' AND
    c.ref_wo_number = d.wo_number AND
    c.wo_number = t.wo_number AND
    c.project = p.project AND
    t.division_type = p.division
) WHERE fiscal_date > ='01-Jan-01';
