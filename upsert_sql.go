package bloomdb

var upsertSql = `
--- 1) INSERT Main Table INTERSECT Temp Table into version table (to be updated)
INSERT INTO {{.Table}}_revisions (
	{{range $i, $e := .Columns}}{{$e}},{{end}}
	bloom_created_at,
	bloom_updated_at,
	bloom_action
	)
  (SELECT 
  	{{range $i, $e := .Columns}}{{$e}},{{end}}
		bloom_created_at,
		'{{.UpdatedAt}}' AS bloom_updated_at,
		'UPDATE' AS bloom_action
  FROM {{.Table}}
  WHERE EXISTS(
    SELECT 1
    FROM {{.Table}}_temp
    WHERE {{.Table}}_temp.id = {{.Table}}.id
    AND {{.Table}}_temp.revision != {{.Table}}.revision));

--- 2) Update Main Table from Temp Table
UPDATE {{.Table}}
SET
{{range $i, $e := .Columns}}{{$e}} = {{$.Table}}_temp.{{$e}},{{end}}
bloom_created_at = '{{.CreatedAt}}'
FROM {{.Table}}_temp
WHERE EXISTS(
    SELECT *
    FROM {{.Table}}_temp
    WHERE {{.Table}}_temp.id = {{.Table}}.id
    AND {{.Table}}_temp.revision != {{.Table}}.revision);

--- 3) Insert New records into Main Table
INSERT INTO {{.Table}} (
{{range $i, $e := .Columns}}{{$e}},{{end}}
bloom_created_at
)
SELECT
{{range $i, $e := .Columns}}{{$e}},{{end}}
'{{.CreatedAt}}' AS bloom_created_at
FROM {{.Table}}_temp 
WHERE EXISTS (
  SELECT * FROM (
    SELECT id FROM {{.Table}}_temp
    EXCEPT
    SELECT id from {{.Table}}) AS f
  WHERE f.id = {{.Table}}_temp.id);
`