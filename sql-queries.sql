-- Unique GHO Holders 

SELECT count(DISTINCT a."user")
FROM (
	SELECT "aave_ghost_mainnet"."borrows"."user" AS "user"
	FROM "aave_ghost_mainnet"."borrows"
	
	UNION ALL
	
	SELECT "aave_ghost_mainnet"."supplies"."user" AS "user"
	FROM "aave_ghost_mainnet"."supplies"
	
	UNION ALL
	
	SELECT "aave_ghost_mainnet"."redeem_underlyings"."user" AS "user"
	FROM "aave_ghost_mainnet"."redeem_underlyings"
	
	UNION ALL
	
	SELECT "aave_ghost_mainnet"."repays"."user" AS "user"
	FROM "aave_ghost_mainnet"."repays"
	) a


-- Unique GHO Holders in Last 60 Days 

WITH cte1
AS (
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."borrows"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."supplies"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."redeem_underlyings"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."repays"
	)
SELECT	count(DISTINCT "user")
FROM cte1
WHERE (date_trunc('week', "timestamp") > now() + interval '-60 Day')


-- Unique GHO Users Over The Time 

WITH cte1
AS (
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."borrows"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."supplies"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."redeem_underlyings"
	
	UNION ALL
	
	SELECT "user"
		,"timestamp"
	FROM "aave_ghost_mainnet"."repays"
	)
SELECT date_trunc('week', "timestamp")
	,count(DISTINCT "user")
FROM cte1
GROUP BY date_trunc('week', "timestamp")

-- Users with Repeat Transactions

SELECT (CAST(DATE_TRUNC('week', ("source"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day') AS "timestamp"
	,"source"."action" AS "action"
	,SUM("source"."count") AS "sum"
FROM (
	SELECT "aave_ghost_mainnet"."user_transactions"."user" AS "user"
		,(CAST(DATE_TRUNC('week', ("aave_ghost_mainnet"."user_transactions"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day') AS "timestamp"
		,"aave_ghost_mainnet"."user_transactions"."action" AS "action"
		,COUNT(*) AS "count"
	FROM "aave_ghost_mainnet"."user_transactions"
	GROUP BY "aave_ghost_mainnet"."user_transactions"."user"
		,(CAST(DATE_TRUNC('week', ("aave_ghost_mainnet"."user_transactions"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day')
		,"aave_ghost_mainnet"."user_transactions"."action"
	ORDER BY "aave_ghost_mainnet"."user_transactions"."user" ASC
		,(CAST(DATE_TRUNC('week', ("aave_ghost_mainnet"."user_transactions"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day') ASC
		,"aave_ghost_mainnet"."user_transactions"."action" ASC
	) AS "source"
WHERE ("source"."count" > 1)
	AND (
		("source"."action" = 'Borrow')
		OR ("source"."action" = 'RedeemUnderlying')
		OR ("source"."action" = 'Repay')
		OR ("source"."action" = 'Supply')
		)
GROUP BY (CAST(DATE_TRUNC('week', ("source"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day')
	,"source"."action"
ORDER BY (CAST(DATE_TRUNC('week', ("source"."timestamp" + INTERVAL '1 day')) AS TIMESTAMP) + INTERVAL '-1 day') ASC
	,"source"."action" ASC

-- Borrow Tx's / 24h

SELECT CAST("aave_ghost_mainnet"."borrows"."timestamp" AS DATE) AS "timestamp"
	,COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."borrows"
WHERE ("aave_ghost_mainnet"."borrows"."timestamp" >= CAST((NOW() + INTERVAL '-2 day') AS DATE))
	AND ("aave_ghost_mainnet"."borrows"."timestamp" < CAST(NOW() AS DATE))
GROUP BY CAST("aave_ghost_mainnet"."borrows"."timestamp" AS DATE)
ORDER BY CAST("aave_ghost_mainnet"."borrows"."timestamp" AS DATE) ASC

-- Supplies Tx's / 24h

SELECT CAST("aave_ghost_mainnet"."supplies"."timestamp" AS date) AS "timestamp", COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."supplies"
WHERE ("aave_ghost_mainnet"."supplies"."timestamp" >= CAST((NOW() + INTERVAL '-2 day') AS date))
   AND ("aave_ghost_mainnet"."supplies"."timestamp" < CAST(NOW() AS date))
GROUP BY CAST("aave_ghost_mainnet"."supplies"."timestamp" AS date)
ORDER BY CAST("aave_ghost_mainnet"."supplies"."timestamp" AS date) ASC

-- Redeem Underlying Tx's / 24h

SELECT CAST("aave_ghost_mainnet"."redeem_underlyings"."timestamp" AS date) AS "timestamp", COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."redeem_underlyings"
WHERE ("aave_ghost_mainnet"."redeem_underlyings"."timestamp" >= CAST((NOW() + INTERVAL '-2 day') AS date))
   AND ("aave_ghost_mainnet"."redeem_underlyings"."timestamp" < CAST(NOW() AS date))
GROUP BY CAST("aave_ghost_mainnet"."redeem_underlyings"."timestamp" AS date)
ORDER BY CAST("aave_ghost_mainnet"."redeem_underlyings"."timestamp" AS date) ASC

-- Repay Tx's / 24h

SELECT CAST("aave_ghost_mainnet"."repays"."timestamp" AS date) AS "timestamp", COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."repays"
WHERE ("aave_ghost_mainnet"."repays"."timestamp" >= CAST((NOW() + INTERVAL '-2 day') AS date))
   AND ("aave_ghost_mainnet"."repays"."timestamp" < CAST(NOW() AS date))
GROUP BY CAST("aave_ghost_mainnet"."repays"."timestamp" AS date)
ORDER BY CAST("aave_ghost_mainnet"."repays"."timestamp" AS date) ASC

-- User Action

SELECT "aave_ghost_mainnet"."user_transactions"."action" AS "action", COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."user_transactions"
GROUP BY "aave_ghost_mainnet"."user_transactions"."action"
ORDER BY "count" DESC, "aave_ghost_mainnet"."user_transactions"."action" ASC

-- Transaction Count

SELECT COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."borrows"

SELECT COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."repays"
  
SELECT COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."supplies"

SELECT COUNT(*) AS "count"
FROM "aave_ghost_mainnet"."redeem_underlyings"

