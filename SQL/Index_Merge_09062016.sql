



/* Create backup of old table */

DROP TABLE IF EXISTS irs.form_index_old_09062016;
CREATE TABLE irs.form_index_old_09062016 LIKE irs.form_index;
INSERT irs.form_index_old_09062016 SELECT * FROM irs.form_index;
/*
3481274 row(s) affected Records: 3481274  Duplicates: 0  Warnings: 0
*/

DROP TABLE IF EXISTS irs.form_index_new_09062016;
CREATE TABLE irs.form_index_new_09062016 LIKE irs.form_index_temp_2;
INSERT irs.form_index_new_09062016 SELECT * FROM irs.form_index_temp_2;

/*
3618506 row(s) affected Records: 3618506  Duplicates: 0  Warnings: 0
*/



SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY url HAVING xx > 1;
/* 21 rows returned.  11.140 sec.  Dupecount = 2 for all. */

SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY dln HAVING xx > 1;
/* 22 rows returned, 71 seconds.  Dupecount = 2 for all.  */

SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY url, dln HAVING xx > 1;
/* 21 rows returned.  206.000 sec.  Dupecount = 2 for all. */

/* Conclusion: if url is duped, then dln is duped, but not necessarially the other way around */

SELECT *, COUNT(*) as xx FROM irs.form_index WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY url, dln HAVING xx > 1;
/* 0 row(s) returned */

SELECT *, COUNT(*) as xx FROM irs.form_index WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY dln HAVING xx > 1;
/* 1 row returned */


CREATE TABLE irs.dupes_to_address_09062016 AS SELECT a.* FROM irs.form_index_temp_2 a, (SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY dln HAVING xx > 1) b WHERE a.dln = b.dln;
/* 44 rows affected */

DELETE a.* FROM irs.form_index_temp_2 a, (SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <> '' and IFNULL(url,'') <> '' GROUP BY dln HAVING xx > 1) b WHERE a.dln = b.dln;
/* 44 rows affected */

SELECT *, COUNT(*) as xx FROM irs.form_index WHERE objectId IS NOT NULL GROUP BY objectId HAVING xx > 1;
/* 29 rows */

SELECT *, COUNT(*) as xx FROM irs.form_index WHERE objectId IS NOT NULL AND url IS NOT NULL GROUP BY objectId HAVING xx > 1;
/* 0 rows */

SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE objectId IS NOT NULL AND IFNULL(url,'') <> '' GROUP BY objectId HAVING xx > 1;
/* 0 rows */

ALTER TABLE irs.form_index ADD INDEX (objectId);
/* 20.860 sec */

ALTER TABLE irs.form_index_temp_2 ADD INDEX (objectId);
/* 27.922 sec */


SELECT a.*, b.* FROM irs.form_index a INNER JOIN irs.form_index_temp_2 b ON a.objectId = b.objectId;
/* 0 rows returned */


SELECT COUNT(*) FROM irs.form_index_temp_2 a LEFT JOIN irs.form_index b ON b.ein = a.ein WHERE b.ein IS NULL;
/*11935 rows returned*/


SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE NOT EXISTS (SELECT 1 FROM irs.form_index WHERE irs.form_index.ein = irs.form_index_temp_2.ein);
/*11935 rows returned*/


SELECT COUNT(*) FROM irs.form_index WHERE isElectronic = 1 AND isAvailable = 1 AND IFNULL(url, '') = '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isElectronic = 1 AND isAvailable = 1 AND IFNULL(url, '') = '';
/* 0 */


SELECT COUNT(*) FROM irs.form_index_temp_2 a INNER JOIN irs.form_index b ON a.ein = b.ein AND a.taxPeriod = b.taxPeriod;
/* 3681204 : more than exist in either.  Not unique */

SELECT COUNT(*) FROM irs.form_index_temp_2 a INNER JOIN irs.form_index b ON a.ein = b.ein AND a.taxPeriod = b.taxPeriod AND a.submittedOn = b.submittedOn;
/* 3491191 */

SELECT *, COUNT(*) as xx FROM irs.form_index WHERE IFNULL(url,'') = '' GROUP BY ein, taxPeriod, submittedOn, updated HAVING xx > 1;
/* 1000 + */

SELECT COUNT(*) AS dupes FROM (
SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 WHERE IFNULL(url,'') = '' GROUP BY ein, taxPeriod, submittedOn, isElectronic, isAvailable, updated HAVING xx > 1) x;
/* 1000 + */

SELECT *, COUNT(DISTINCT url) as xx FROM irs.form_index WHERE IFNULL(url, '') <> '' GROUP BY ein, taxPeriod HAVING xx > 1;
/* 1000 + */

SELECT *, COUNT(DISTINCT url) as xx FROM irs.form_index WHERE IFNULL(url, '') <> '' GROUP BY ein, taxPeriod, submittedOn HAVING xx > 1;
/* 260 */

SELECT *, COUNT(DISTINCT url) as xx FROM irs.form_index WHERE IFNULL(url, '') <> '' GROUP BY ein, taxPeriod, submittedOn, updated HAVING xx > 1;
/* 251 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE IFNULL(url,'') <> '' AND IFNULL(dln,'') = '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isAvailable = 1 AND IFNULL(dln,'') = '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isAvailable = 1 AND IFNULL(url,'') = '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isAvailable = 0 AND IFNULL(dln,'') <> '';
/* 85859 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isAvailable = 0 AND IFNULL(url,'') <> '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isElectronic = 0 AND IFNULL(dln,'') <> '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_temp_2 WHERE isElectronic = 1 AND IFNULL(dln,'') = '';

SELECT *, COUNT(DISTINCT isElectronic) as xx FROM irs.form_index GROUP BY ein, taxPeriod, submittedOn HAVING xx > 1;

SELECT MIN(dln) FROM irs.form_index_temp_2 WHERE IFNULL(dln,'') <>'';

/*
ALTER TABLE  `form_index` ADD  `dupe` TINYINT( 4 ) NULL DEFAULT NULL COMMENT  'Flag if duplicate to be ignored',
ADD INDEX (  `dupe` ) ;


ALTER TABLE  `irs`.`form_index_temp_2` ADD  `dupe` TINYINT( 4 ) NULL DEFAULT NULL COMMENT  'Flag if duplicate to be ignored',
ADD INDEX (  `dupe` ) ;
*/

SELECT COUNT(*) FROM irs.form_index a LEFT JOIN irs.form_index_temp_2 b ON b.ein = a.ein AND b.taxPeriod = a.taxPeriod AND b.submittedOn = a.submittedOn AND b.isElectronic = a.isElectronic AND b.isAvailable = a.isAvailable AND b.updated = a.updated WHERE b.ein IS NULL;
/* 1966497 */

SELECT COUNT(*) FROM irs.form_index a LEFT JOIN irs.form_index_temp_2 b ON b.ein = a.ein AND b.taxPeriod = a.taxPeriod AND b.submittedOn = a.submittedOn AND b.isElectronic = a.isElectronic AND b.isAvailable = a.isAvailable WHERE b.ein IS NULL;
/* 279 */

SELECT COUNT(*) FROM irs.form_index a LEFT JOIN irs.form_index_temp_2 b ON b.ein = a.ein AND b.taxPeriod = a.taxPeriod AND b.submittedOn = a.submittedOn AND b.isElectronic = a.isElectronic AND b.isAvailable = a.isAvailable AND b.updated = a.updated AND b.url = a.url WHERE b.ein IS NULL;
/* 1966497 */

SELECT COUNT(*) AS dupes FROM (
SELECT *, COUNT(*) as xx FROM irs.form_index_temp_2 GROUP BY dln HAVING xx > 1) x;



INSERT IGNORE INTO irs.form_index (`updated`, `ein`, `url`,
 `submittedOn`, `taxPeriod`, `dln`, `isElectronic`, `isAvailable`,
 `formType`, `year`, `version`, `objectId`) SELECT `updated`, `ein`, `url`,
 `submittedOn`, `taxPeriod`, `dln`, `isElectronic`, `isAvailable`,
 `formType`, `year`, `version`, `objectId` FROM irs.form_index_temp_2 WHERE IFNULL(url,'') <> '';
 
 
 DELETE FROM irs.form_index WHERE IFNULL(url,'')='' AND IFNULL(queueId,'') = '';
 INSERT INTO irs.form_index (`updated`, `ein`, `url`,
 `submittedOn`, `taxPeriod`, `dln`, `isElectronic`, `isAvailable`,
 `formType`, `year`, `version`, `objectId`) SELECT `updated`, `ein`, NULL,
 `submittedOn`, `taxPeriod`, `dln`, `isElectronic`, `isAvailable`,
 `formType`, `year`, `version`, `objectId` FROM irs.form_index_temp_2 WHERE IFNULL(url,'')='';
 
 

 

SELECT COUNT(*) FROM irs.form_index_temp_2 a LEFT JOIN irs.form_index_backup b ON b.url = a.url WHERE b.ein IS NULL AND IFNULL(a.url, '') <> '';



SELECT COUNT(DISTINCT url) FROM irs.form_index_backup;
/*1515057*/

SELECT COUNT(DISTINCT url) FROM irs.form_index;
/*1515057*/

SELECT COUNT(DISTINCT url) FROM irs.form_index_temp_2;
/* 1514779 */
SELECT COUNT(DISTINCT url) FROM irs.form_index_new_09062016;
/* 1514802 */

SELECT COUNT(DISTINCT url) FROM irs.form_index_old_09062016;
/* 1515057 */

SELECT COUNT(*) FROM irs.form_index_new_09062016 a LEFT JOIN irs.form_index_old_09062016 b ON b.url = a.url WHERE b.ein IS NULL AND IFNULL(a.url, '') <> '';
/* 0 */

SELECT COUNT(*) FROM irs.form_index_old_09062016 a LEFT JOIN irs.form_index_new_09062016 b ON b.url = a.url WHERE b.ein IS NULL AND IFNULL(a.url, '') <> '';
/* 256 */


SELECT COUNT(DISTINCT url) FROM irs_08232016.form_index;


SELECT COUNT(*) FROM irs.dupes_to_address a LEFT JOIN irs.form_index b ON b.url = a.url WHERE b.ein IS NULL AND IFNULL(a.url, '') <> '';
/* 0 */

/* SCRATCH/EXPERIMENTAL */


/* TODO: Populate dupes columns */


/* Select nondupe entries out into scratch table. */

DROP TABLE IF EXISTS scratch.atdTMP_idx_load;
CREATE TABLE scratch.atdTMP_idx_load AS SELECT * FROM irs.form_index WHERE dupe IS NULL;
ALTER TABLE scratch.atdTMP_idx_load ADD temp_id INT PRIMARY KEY AUTO_INCREMENT;
ALTER TABLE scratch.atdTMP_idx_load ADD UNIQUE(id);

/* TODO: INSERT/UPDATE into scratch table, setting new rows to a NULL id */


/* TODO: INSERT/UPDATE from scratch table into the index based on the id */
/* PROBLEM WHEN INSERT/UPDATING TO TABLE WITH MULTIPLE UNIQUE INDEXES! */

/* UPDATE .... SELECT ... FROM scratch.atdTMP_idx_load WHERE scratch.atdTMP_idx_load.id IS NOT NULL */
/* INSERT INTO irs.form_index ....... SELECT .... FROM scratch.atdTMP_idx_load WHERE scratch.atdTMP_idx_load.id IS NULL */

