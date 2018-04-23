SELECT * FROM
(
  SELECT
    Decode(Grouping(s.username), 1, 'All users', s.username) username,
    Decode(Grouping(machine), 1, 'All machines', machine) machine,
    Decode(Grouping(s.program), 1, 'All programs', s.program) program,
    Decode(Grouping(s.status), 1, 'All statuses', s.status) status,
    COUNT(*) cnt,
    Grouping_ID(s.username, machine, s.program, s.status) g
  FROM v$session s
  WHERE s.username IS NOT NULL
  AND program NOT IN ('sqlnav3.exe','sqlplusw.exe','TOAD.exe')
  GROUP BY ROLLUP(s.username, machine, s.program, s.status)
)
WHERE g IN (0, 15)
ORDER BY g
