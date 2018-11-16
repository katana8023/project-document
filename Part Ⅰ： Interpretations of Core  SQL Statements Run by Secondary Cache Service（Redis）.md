## Part Ⅰ： Interpretations of Core  SQL Statements Run by Secondary Cache Service（Redis）

##### Amber.Guo
##### 2018/11/16 10:00:00
---
### *1. For Construction Industry*
 __1.1 Package:  com.sunland.mas.scheduled.*__

- #### EnterpriseIdScheduler.class

```java
// get all enterprise ID
// m: fetchJTJSCompanyID()

String sql = "
SELECT
	tid
FROM
	meta_credit_enterprise
WHERE
	relatedId = '0'
ORDER BY
	timeUpdated DESC";
// That the limit condition here is "relatedId = '0'", 
// is because the data with relatedId != '0' all has redundant records
// and the value of relatedId refers to the previous data.
// In other words, for thoes containing more than 1 record, 
// this condition would filter out the old data and fetch the newest updated instead
```

```java
// m: 
// get enterprise name listed on blacklist

String sql = "
SELECT DISTINCT
	enterName
FROM
	ett_blacklist_enterprise
WHERE
	enventType = 2";
// Data with condition "enventType = 2" refers to contruction industry,
// while 1 refers to transport industry
```

```java
// m: pushCompanyBlacklist2Redis()
// get all representatives listed on balcklist

String sql = "
SELECT DISTINCT
	represent
FROM
	ett_blacklist_enterprise
WHERE
	enventType = 2";
// ditto
```

---
- #### PersonnelIdScheduler.class

```java
// m: fetchJTJSPersonnelID()
// get all personnel ID

String sql = "
SELECT
	p.tid
FROM
	meta_personnel p
RIGHT JOIN (
	SELECT
		MAX(timeUpdated) AS timeUpdated,
		identityCard
	FROM
		meta_personnel
	WHERE
		remarks = ''
	AND enterpriseId != '0'
	GROUP BY
		identityCard
) a ON a.identityCard = p.identityCard
AND a.timeUpdated = p.timeUpdated";
// In order to avoid redundance,
// condition "remark = ''"(hereafter CdtA) is compulsary,
// because the colume `remark` without CdtA shows "系统自动：2016-07-14 15:56:27企业清退，人员解职".
// And "enterpriseId != '0'"(hereafter CdtB) is also critical,
// because it is required that each personnel must be related to the company.
// "GROUP BY identityCard"(hereafter CdtC) is crutial,
// because the one person could possess several `tid`,
// in this case, `tid` cannot be a unique code of a person(but of a record instead),
// therefore `identityCard` is selected instead.
```
---

- #### ProjectScheduler.class

```java
// get all project ID
// m: fetchAllProjectID()

String sql = "
SELECT
	tid
FROM
	meta_credit_project";

// get project ID with section type - construct
// m: fetchContructProjectID()

String sql = "
SELECT
	tid
FROM
	meta_project_construct_section";

// ditto - design 
// m: fetchDesignProjectID()

String sql = "
SELECT
	tid
FROM
	meta_project_design_section";

// ditto - inspection
// m: fetchInspectProjectID()

String sql = "
SELECT
	tid
FROM
	meta_project_inspection_section";

// ditto - supervisor
// m: fetchSupvProjectID()

String sql = "
SELECT
	tid
FROM
	meta_project_supervisor_section";
	
```
---

__1.2 Path: classpath: resources/mapper1.*__

- #### PositionMapper.xml

```java
// get personnel position infomation
// m(id): getPositionInfo
// return: com.sunland.mas.redis.cachePojo.PositionMsg

String sql = "
SELECT
	personnelId,
	GROUP_CONCAT(DISTINCT positionName) AS positionName
FROM
	meta_project_personnel
GROUP BY
	personnelId";
//
```
---

- #### EnterpriseInfoMapper.xml

```java
// get latest updated credit info
// m(id): getCreditAndScore
// return: com.sunland.mas.redis.cachePojo.CreditScoreAndGrade

SELECT
	s2.enterpriseId AS enterpriseId,
	s1.creditGrade AS creditGrade,
	s1.creditScore AS creditScore
FROM
	meta_enterprise_credit_score s1
RIGHT JOIN (
	SELECT
		MAX(DISTINCT timeUpdated) AS timeUpdated,
		enterpriseId
	FROM
		meta_enterprise_credit_score
	WHERE TRUE
	<if test="idList == null or idList.size == 0">AND FALSE</if>  
	<if test="idList != null and idList.size > 0">
		AND enterpriseId IN  
	<foreach collection="idList" index="index" item="idList" open="(" close=")" separator=","> #{idList}</foreach>  
	GROUP BY  
		enterpriseId</if>
	) s2 ON s1.timeUpdated = s2.timeUpdated
	AND s1.enterpriseId = s2.enterpriseId
	<if test="creditGrade == 1">AND s1.creditGrade LIKE CONCAT('%','AA','%')</if>  
	<if test="creditGrade == 2">AND s1.creditGrade LIKE CONCAT('%','A','%')</if>  
	<if test="creditGrade == 3">AND s1.creditGrade LIKE CONCAT('%','B','%')</if>  
	<if test="creditGrade == 4">AND s1.creditGrade LIKE CONCAT('%','C','%')</if>  
	<if test="need == 1">LIMIT #{start},#{pageSize}</if>
	
```
```java
// calculate average value of credit score 
// m(id): doAvg
// return: BigDecimal

SELECT  
 AVG(s1.creditScore) AS avgScore  
FROM  
    meta_enterprise_credit_score s1  
  RIGHT JOIN (  
	 SELECT  
		MAX(DISTINCT timeUpdated) AS timeUpdated,  
		enterpriseId  
	  FROM  
		meta_enterprise_credit_score  
	 WHERE TRUE  
	 <if test="idList == null or idList.size == 0">AND FALSE</if>  
	 <if test="idList != null and idList.size > 0">  
	AND enterpriseId IN  
	<foreach collection="idList" item="idList" index="index" open="(" close=")" separator=",">#{idList}</foreach>  
	 GROUP BY  
		enterpriseId  
	 </if>  
  ) s2 ON s1.timeUpdated = s2.timeUpdated  
  AND s1.enterpriseId = s2.enterpriseId

```
### *2. For Transport Industry*
__2.1 Package:  com.sunland.mas.scheduled.*__

- #### EnterpriseIdScheduler.class

```java
// get all transport companies' ID
// m: fetchYSCompanyID()
// return List<String>
String sql = "
SELECT
	a.comId
FROM
	(
		SELECT
			comId,
			oName,
			represName
		FROM
			meta_owner
		WHERE
			comId IN (
				SELECT DISTINCT
					comId
				FROM
					meta_owner_businesscope
				WHERE
					busiScopeId IN (
						SELECT
							scopeId
						FROM
							meta_type_ownerbusiscope
						WHERE
							scopeTypeCode = 1
						OR scopeTypeCode = 9
					)
			)
		AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
		UNION
			(
				SELECT
					comId,
					oName,
					represName
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 2
							)
					)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		UNION
			(
				SELECT
					comId,
					oName,
					represName
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 3
							)
					)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		UNION
			(
				SELECT
					comId,
					oName,
					represName
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 4
							)
					)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
	) a
WHERE
	a.oName NOT LIKE '%调试件%'
OR a.represName NOT LIKE '%调试件%'
OR a.represName NOT LIKE '%张三%'
OR a.oName NOT LIKE '%测试%'
OR a.represName NOT LIKE '%测试%'
ORDER BY
	LENGTH(a.oName) DESC";

// the design of this sql has THREE purpose:
// 1. use UNION in order to filter out the redundant `comId`;
// 2. TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0: deliminate those with their bussiness certifications expired;
// 3. LENGTH(a.oName) DESC: give the privelige to those companies who's name is not an individual name

```
```java
// get KY(客运) companies' ID
// m: fetchKYCompanyID()
// return List<String>

String sql = "
SELECT
	comId
FROM
	meta_owner
WHERE
	comId IN (
		SELECT DISTINCT
			comId
		FROM
			meta_owner_businesscope
		WHERE
			busiScopeId IN (
				SELECT DISTINCT
					scopeId
				FROM
					meta_type_ownerbusiscope
				WHERE
					scopeTypeCode = 1
				OR scopeTypeCode = 9
			)
	)
AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
AND (
	oName NOT LIKE '%调试件%'
	OR represName NOT LIKE '%调试件%'
	OR represName NOT LIKE '%张三%'
	OR oName NOT LIKE '%测试%'
	OR represName NOT LIKE '%测试%'
)
ORDER BY
	LENGTH(oName) DESC";
// the design of this sql has the same purpose as it's mentioned above,
// so HY(货运),JP(驾培),WX(维修) are with no extra explainations
```
---
- #### PunishmentCompanyIdScheduler.class

```java
// get all transport punishment companies' IDs
// m: fetchPunishComID()
// return List<String>

String sql = "
SELECT
	vehicleUnitId
FROM
	ett_law_punishment
WHERE
	vehicleUnitId IN (
		SELECT
			comId
		FROM
			meta_owner
		WHERE
			comId IN (
				SELECT DISTINCT
					comId
				FROM
					meta_owner_businesscope
				WHERE
					busiScopeId IN (
						SELECT
							scopeId
						FROM
							meta_type_ownerbusiscope
						WHERE
							scopeTypeCode = 1
					)
			)
		AND (
			oName NOT LIKE '%测试%'
			OR represName NOT LIKE '%调试件%'
			OR represName NOT LIKE '%张三%'
			OR oName NOT LIKE '%测试%'
			OR represName NOT LIKE '%测试%'
		)
		AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
		UNION
			(
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 2
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		UNION
			(
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 3
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		UNION
			(
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 4
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
	)
AND PERIOD_DIFF(
	DATE_FORMAT(NOW(), '%Y%m'),
	DATE_FORMAT(timeUpdated, '%Y%m')
) <= 6
AND licenseNum != ''";


// get KY punishment companies' IDs
// m: fetchKYPunishComID()
// ditto

String sql1 = "
SELECT
	vehicleUnitId
FROM
	ett_law_punishment
WHERE
	vehicleUnitId IN (
		SELECT
			comId
		FROM
			meta_owner
		WHERE
			comId IN (
				SELECT DISTINCT
					comId
				FROM
					meta_owner_businesscope
				WHERE
					busiScopeId IN (
						SELECT
							scopeId
						FROM
							meta_type_ownerbusiscope
						WHERE
							scopeTypeCode = 1
						OR scopeTypeCode = 9
					)
			)
		AND (
			oName NOT LIKE '%测试%'
			OR represName NOT LIKE '%调试件%'
			OR represName NOT LIKE '%张三%'
			OR oName NOT LIKE '%测试%'
			OR represName NOT LIKE '%测试%'
		)
		AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
	)
AND PERIOD_DIFF(
	DATE_FORMAT(NOW(), '%Y%m'),
	DATE_FORMAT(timeUpdated, '%Y%m')
) <= 6
AND licenseNum != ''";

// AND PERIOD_DIFF(DATE_FORMAT(NOW(), '%Y%m'),DATE_FORMAT(timeUpdated, '%Y%m')) <=6: get puishment records within 6 months
```



- #### PersonnelIdScheduler.class

```java
// get all transport personnel ID
// m: fetchYSPersonnelID()
// return List<String>

String sql = "
SELECT
	eDcode
FROM
	meta_employee_cert
WHERE
	TO_DAYS(certiDateEnd) - TO_DAYS(NOW()) > 0
GROUP BY
	eDcode";

// get KY personnel ID
// m: fetchKYPersonnelID()
// return ditto
Strign sql1 = "
SELECT
	eDcode
FROM
	meta_employee_cert
WHERE
	TO_DAYS(certiDateEnd) - TO_DAYS(NOW()) > 0
AND (
	cType = '道路旅客运输'
	OR category = '道路旅客运输及客运站经理人'
)
GROUP BY
	eDcode";

// get HY personnel ID
// m: fetchHYPersonnelID()
// ditto
String sql2 = "
SELECT
	eDcode
FROM
	meta_employee_cert
WHERE
	TO_DAYS(certiDateEnd) - TO_DAYS(NOW()) > 0
AND (
	cType = '道路货物运输'
	OR cType = '道路危险货物运输'
	OR category = '道路货物运输及站场经理人'
)
GROUP BY
	eDcode";

// get WX personnel ID
// m: fetchWXPersonnelID()
// ditto
String sql3 = "
SELECT
	eDcode
FROM
	meta_employee_cert
WHERE
	TO_DAYS(certiDateEnd) - TO_DAYS(NOW()) > 0
AND (
	cType = '机动车检测维修'
	OR cType = '其他道路运输'
	OR category = '机动车检测维修经理人'
)
GROUP BY
	eDcode";

// get JP personnel ID
// m: fetchJPPersonnelID()
// ditto
String sql4 = "
SELECT
	eDcode
FROM
	meta_employee_cert
WHERE
	TO_DAYS(certiDateEnd) - TO_DAYS(NOW()) > 0
AND category = '机动车驾驶培训经理人'
GROUP BY
	eDcode";

```
- #### VehicleCountScheduler.class

```java
// get all vehicles count
// m: countAllYSCar()
// return int

String sql = "
SELECT
	COUNT(big.comNum) AS count
FROM
	(
		SELECT DISTINCT
			comNum
		FROM
			meta_car_vehicle
		WHERE
			comId IN (
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 1
								OR scopeTypeCode = 9
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
				UNION
					(
						SELECT
							comId
						FROM
							meta_owner
						WHERE
							comId IN (
								SELECT DISTINCT
									comId
								FROM
									meta_owner_businesscope
								WHERE
									busiScopeId IN (
										SELECT
											scopeId
										FROM
											meta_type_ownerbusiscope
										WHERE
											scopeTypeCode = 2
									)
							)
						AND (
							oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%调试件%'
							OR represName NOT LIKE '%张三%'
							OR oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%测试%'
						)
						AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
					)
				UNION
					(
						SELECT
							comId
						FROM
							meta_owner
						WHERE
							comId IN (
								SELECT DISTINCT
									comId
								FROM
									meta_owner_businesscope
								WHERE
									busiScopeId IN (
										SELECT
											scopeId
										FROM
											meta_type_ownerbusiscope
										WHERE
											scopeTypeCode = 3
									)
							)
						AND (
							oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%调试件%'
							OR represName NOT LIKE '%张三%'
							OR oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%测试%'
						)
						AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
					)
				UNION
					(
						SELECT
							comId
						FROM
							meta_owner
						WHERE
							comId IN (
								SELECT DISTINCT
									comId
								FROM
									meta_owner_businesscope
								WHERE
									busiScopeId IN (
										SELECT
											scopeId
										FROM
											meta_type_ownerbusiscope
										WHERE
											scopeTypeCode = 4
									)
							)
						AND (
							oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%调试件%'
							OR represName NOT LIKE '%张三%'
							OR oName NOT LIKE '%测试%'
							OR represName NOT LIKE '%测试%'
						)
						AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
					)
			)
		AND (rStatus = 10 OR rStatus = 21)
		UNION
			SELECT DISTINCT
				comNum
			FROM
				meta_vehicle
			WHERE
				comId IN (
					SELECT
						comId
					FROM
						meta_owner
					WHERE
						comId IN (
							SELECT DISTINCT
								comId
							FROM
								meta_owner_businesscope
							WHERE
								busiScopeId IN (
									SELECT
										scopeId
									FROM
										meta_type_ownerbusiscope
									WHERE
										scopeTypeCode = 1
									OR scopeTypeCode = 9
								)
						)
					AND (
						oName NOT LIKE '%测试%'
						OR represName NOT LIKE '%调试件%'
						OR represName NOT LIKE '%张三%'
						OR oName NOT LIKE '%测试%'
						OR represName NOT LIKE '%测试%'
					)
					AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
					UNION
						(
							SELECT
								comId
							FROM
								meta_owner
							WHERE
								comId IN (
									SELECT DISTINCT
										comId
									FROM
										meta_owner_businesscope
									WHERE
										busiScopeId IN (
											SELECT
												scopeId
											FROM
												meta_type_ownerbusiscope
											WHERE
												scopeTypeCode = 2
										)
								)
							AND (
								oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%调试件%'
								OR represName NOT LIKE '%张三%'
								OR oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%测试%'
							)
							AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
						)
					UNION
						(
							SELECT
								comId
							FROM
								meta_owner
							WHERE
								comId IN (
									SELECT DISTINCT
										comId
									FROM
										meta_owner_businesscope
									WHERE
										busiScopeId IN (
											SELECT
												scopeId
											FROM
												meta_type_ownerbusiscope
											WHERE
												scopeTypeCode = 3
										)
								)
							AND (
								oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%调试件%'
								OR represName NOT LIKE '%张三%'
								OR oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%测试%'
							)
							AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
						)
					UNION
						(
							SELECT
								comId
							FROM
								meta_owner
							WHERE
								comId IN (
									SELECT DISTINCT
										comId
									FROM
										meta_owner_businesscope
									WHERE
										busiScopeId IN (
											SELECT
												scopeId
											FROM
												meta_type_ownerbusiscope
											WHERE
												scopeTypeCode = 4
										)
								)
							AND (
								oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%调试件%'
								OR represName NOT LIKE '%张三%'
								OR oName NOT LIKE '%测试%'
								OR represName NOT LIKE '%测试%'
							)
							AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
						)
				)
	) big";

```
```java
// get KY vehicle count
// m: countKYCar()
// return int

String sql = "
SELECT
	COUNT(big.comNum) AS count
FROM
	(
		SELECT DISTINCT
			comNum
		FROM
			meta_car_vehicle
		WHERE
			comId IN (
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 1
								OR scopeTypeCode = 9
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		AND (rStatus = 10 OR rStatus = 21)
		UNION
			SELECT DISTINCT
				comNum
			FROM
				meta_vehicle
			WHERE
				comId IN (
					SELECT
						comId
					FROM
						meta_owner
					WHERE
						comId IN (
							SELECT DISTINCT
								comId
							FROM
								meta_owner_businesscope
							WHERE
								busiScopeId IN (
									SELECT
										scopeId
									FROM
										meta_type_ownerbusiscope
									WHERE
										scopeTypeCode = 1
									OR scopeTypeCode = 9
								)
						)
					AND (
						oName NOT LIKE '%测试%'
						OR represName NOT LIKE '%调试件%'
						OR represName NOT LIKE '%张三%'
						OR oName NOT LIKE '%测试%'
						OR represName NOT LIKE '%测试%'
					)
					AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
				)
			AND rStatus = 0
	) big";

// get HY vehicle count
// m: countHYCar()
// ditto

String sql1 = "
SELECT
	COUNT(big.comNum) AS count
FROM
	(
		SELECT DISTINCT
			comNum
		FROM
			meta_vehicle
		WHERE
			comId IN (
				SELECT
					comId
				FROM
					meta_owner
				WHERE
					comId IN (
						SELECT DISTINCT
							comId
						FROM
							meta_owner_businesscope
						WHERE
							busiScopeId IN (
								SELECT
									scopeId
								FROM
									meta_type_ownerbusiscope
								WHERE
									scopeTypeCode = 2
							)
					)
				AND (
					oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%调试件%'
					OR represName NOT LIKE '%张三%'
					OR oName NOT LIKE '%测试%'
					OR represName NOT LIKE '%测试%'
				)
				AND TO_DAYS(validDateEnd) - TO_DAYS(NOW()) > 0
			)
		AND rStatus = 0
	) big";
	
// JP、WX are the same as HY

```
---
### *3. For Port Industry*
__2.1 Package:  com.sunland.mas.scheduled.*__

- #### EnterpriseIdScheduler.class

```java
// get all port companies' IDs
// m: fetchPortCompanyID()
// return List<String> 
String sql = "
SELECT
	e.enterpriseId
FROM
	meta_cert_enterprise e,
	(
		SELECT
			transactor,
			MAX(timeUpdated) AS timeUpdated
		FROM
			`meta_cert_enterprise`
		WHERE
			majorPort = 1
		AND operator != '测试'
		AND address != '123'
		AND transactor NOT LIKE '%测试%'
		AND transactor != '123'
		GROUP BY
			transactor
		ORDER BY
			timeUpdated
	) a
WHERE
	e.transactor = a.transactor
AND e.timeUpdated = a.timeUpdated";

```
---
- #### ShipScheduler.class

```java
// get port compamies with ship number
// m:  getComWithShipNum()
// return List<CompanyMsg>

String sql = "
SELECT
	en.transactor AS enterpriseName,
	COUNT(DISTINCT wa.shipCode) AS shipNum
FROM
	meta_cert_enterprise en
INNER JOIN meta_cert_waterway wa ON en.transactor = wa.enterpriseName
GROUP BY
	en.transactor";
```