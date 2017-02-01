

SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY url HAVING xx > 1;
/* 21 rows returned.  11.140 sec.  Dupecount = 2 for all. */

SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY dln HAVING xx > 1;

/* Create backup of old table */

DROP TABLE IF EXISTS irs.form_index_old_08312016;
CREATE TABLE irs.form_index_old_08312016 LIKE irs.form_index;
INSERT irs.form_index_old_08312016 SELECT * FROM irs.form_index;



SELECT old_index.*, new_index.* FROM irs.form_index_old_08312016 AS old_index _____ irs.form_index_temp AS new_index ON ____;

