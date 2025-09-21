create or replace view DB_CONSUMER_BANKING.WGORWCA.XV_TXF_CDH345_GOREWARDS_CREDIT_ACCRUAL_D9318_T01_D
as
SELECT 
    29 AS RetailerId,
    cycles.rrc_store_code AS StoreID, 
    -75 AS PosID,
    280010000 AS CashierID,
    0 AS TicketTotal,
    TO_CHAR(DATEADD(DAY, -1, CAST(cycles.generatedatetime AS TIMESTAMP)), 'YYYY-MM-DD 00:00:00') AS BusinessDate,
    TO_CHAR(DATEADD(DAY, -1, CAST(cycles.generatedatetime AS TIMESTAMP)), 'YYYY-MM-DD 00:00:00') AS StartDateTime,
    ROW_NUMBER() OVER (PARTITION BY TO_CHAR(cycles.generatedatetime, 'YYYYMMDD') 
                       ORDER BY people.firstname, people.lastname, 
                                COALESCE(TRIM(LKP.rrc_cardnumber), extension.value), cycles.acct_num) AS TransID,
    COALESCE(TRIM(LKP.rrc_cardnumber), extension.value) AS CardID, 
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS ServerDate,
    CAST(cycles.amount AS DECIMAL(15,0)) AS EarnValue,
    TRIM(people.firstname) AS FirstName,
    TRIM(people.lastname) AS LastName,
    CAST(people.dob AS DATE) AS BirthDate,
    address.email AS EmailAddress,
    CASE
        WHEN address.mobile IS NULL THEN ''
        ELSE TRIM(address.mobile)
    END AS MobileNumber,
    7 AS CustomerTier,
    cycles.rewards_acct_num,
    cycles.acct_num, 
    extension.value AS tsys_cardid,
    TO_CHAR(cycles.generatedatetime, 'YYYYMMDD') AS generatedatetime,
    TO_CHAR(cycles.cycledate, 'YYYYMMDD') AS cycle_date,
   TO_CHAR(DATEADD(DAY, -1, cycles.generatedatetime), 'YYYYMMDD') AS BusinessDate_PT

	
	
FROM
(SELECT --distinct
        LKP.rrc_store_code,
        TRIM(acct_rewards.numberx) AS rewards_acct_num,
        TRIM(acct.numberx) AS acct_num,
        acct.primarycardserno,
        acct.custserno,
        stmt.cycledate,
        stmt.generatedatetime,
        stmt.biz_date ,
        DATEADD(DAY, stmt.cycledays * -1, stmt.billingdate) AS prev_Billing_Date,
        stmt.totalcredits AS amount
    FROM DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CSTATEMENTS_HIST AS stmt 
    JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CACCOUNTS AS ACCT_REWARDS
        ON stmt.caccserno = acct_rewards.serno
    JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CACCOUNTS AS ACCT
        ON TRIM(acct.numberx) = REGEXP_REPLACE(acct_rewards.numberx, '[^0-9]', '')
        AND acct.as_of_date = stmt.biz_date
        --TO_VARCHAR(generatedatetime, 'yyyyMMdd') = stmt.biz_date
        AND acct.accounttype = 'F'
    JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_PRODUCTS AS prod
        ON acct_rewards.product = prod.serno
    INNER JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_ODS_LKP_GOREWARDS_PRODUCTS LKP ---
        ON prod.shortcode = LKP.PRODUCT_CODE
        AND LKP.PRODUCT = 'CREDIT'
    WHERE acct_rewards.accounttype = 'R'
        AND CAST(stmt.totalcredits AS INT) <> 0
)AS cycles 


LEFT JOIN 
(
    SELECT * 
    FROM DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CEXTENSION AS A
    WHERE fieldno = 15001 
        AND tabindicator = 'C'
)AS  extension
ON 
cycles.primarycardserno = extension.rowserno 

LEFT JOIN
(
    SELECT a.product, a.serno, a.peopleserno,a.as_of_date
    FROM DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CCUSTOMERS AS A  
     
) AS customers

ON 
customers.serno = cycles.custserno
LEFT JOIN 
(
    SELECT a.* 
    FROM DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_PEOPLE AS A 
    WHERE a.legalentity = 0
)AS  people

ON 
customers.peopleserno = people.serno
LEFT JOIN 
(
    SELECT 
        a.rowserno,
        a.addresstype,
        b.*,
        ROW_NUMBER() OVER (PARTITION BY a.rowserno ORDER BY CASE WHEN b.email IS NULL THEN 0 ELSE 1 END DESC) AS rn 
    FROM DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CADDRESSLINKS AS a 
    JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_TCTDBS_CADDRESSES AS b
        ON b.serno = a.addressserno
    WHERE a.tabindicator = 'P'
        AND SUBSTR(a.addresstype, 2, 1) = '1'
) AS address

ON 
people.serno = address.rowserno
AND address.rn = 1
LEFT JOIN DB_CONSUMER_BANKING.BGORWCA_D.V_GOREWARDS_MASTER_LIST AS LKP 
ON TRIM(LKP.rrc_oldcardno) = TRIM(
    CASE 
        WHEN SUBSTR(extension.value, 1, 1) = '0' THEN SUBSTR(extension.value, 2, 10)
        ELSE extension.value 
    END
);	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
------------------------------------------------------------------------------------------------------------------------------


create or replace view 
DB_CONSUMER_BANKING.WGORWCA.XV_EXP_CDH345_GOREWARDS_CREDIT_ACCRUAL_D9318_T01_D
as
Select 
	RetailerId,StoreID,PosID,CashierID,TicketTotal, BusinessDate,StartDateTime,TransID,
	CardID,ServerDate,EarnValue,FirstName,LastName,BirthDate,EmailAddress,MobileNumber,CustomerTier 
FROM DB_CONSUMER_BANKING.TGORWCA.T_GOREWARDS_CREDIT_ACCRUAL 
WHERE 
businessdate_pt = '${cycle_date}' 
	and cardid is not null
	and regexp_replace(cardid,'[^0-9]','') <> ''
