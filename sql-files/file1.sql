USE realtime_hudi_api;

(
WITH base AS (
  SELECT
    m.id AS merchant_id,
    m.name AS merchant_name,
    m.email AS merchant_email,
    m.category2 AS merchant_category,
    m.category AS mcc,
    md.business_category,
    md.contact_name,
    md.contact_email,
    md.business_website,
    md.contact_mobile,
    md.business_dba,
    md.activation_status,
    CASE
      WHEN md.business_type = '1' THEN 'Proprietorship'
      WHEN md.business_type = '2' THEN 'Individual'
      WHEN md.business_type = '3' THEN 'Partnership'
      WHEN md.business_type = '4' THEN 'Private Limited'
      WHEN md.business_type = '5' THEN 'Public Limited'
      WHEN md.business_type = '6' THEN 'LLP'
      WHEN md.business_type = '7' THEN 'NGO'
      WHEN md.business_type = '8' THEN 'Educational Institutes'
      WHEN md.business_type = '9' THEN 'Trust'
      WHEN md.business_type = '10' THEN 'Society'
      WHEN md.business_type = '11' THEN 'Not yet registered'
      WHEN md.business_type = '12' THEN 'Other'
    END AS business_type,
    CASE
      WHEN m.id = 'Gws4uCpSEVCvro' THEN 0
      WHEN m.email LIKE '%@razorpay%' THEN 1
      WHEN m.email LIKE '%qautomationusersdet%' THEN 1
      WHEN lower(m.name) LIKE '%razorpay%' THEN 1 --Including New Onboarding Flow Test Merchants
      WHEN m.email LIKE 'solankisoham8+oneca%' THEN 1
      WHEN m.email LIKE 'rsingh669+oneca%' THEN 1 --Including Skip DWT Test Merchants
      WHEN m.email LIKE 'alishagupta152+skipdwt%' THEN 1 --Including Business PAN Test Merchants
      WHEN m.email LIKE 'alishagupta152+bizpan%' THEN 1
      ELSE 0
    END AS is_test_merchant
  FROM
    merchants m
    LEFT JOIN (
      SELECT
        DISTINCT(merchant_id)
      FROM
        realtime_hudi_api.banking_accounts ba
      WHERE
        merchant_id IS NOT NULL
      UNION
      SELECT
        DISTINCT(b.merchant_id)
      FROM
        realtime_prod_banking_accounts.banking_accounts ba
        LEFT JOIN realtime_prod_banking_accounts.businesses b ON b.id = ba.business_id
      WHERE
        b.merchant_id IS NOT NULL
    ) ba_base ON m.id = ba_base.merchant_id
    AND m.business_banking = 0
    LEFT JOIN (
      SELECT
        account_merchant_id__c
      FROM
        dbt_prod_salesforce.opportunity_owner
      WHERE
        type IN (
          'RazorpayX',
          'Current_Account',
          'Payout_Link',
          'Tax_Payment',
          'Vendor_Payout'
        )
      GROUP BY
        1
    ) oov ON oov.account_merchant_id__c = m.id
    LEFT JOIN realtime_hudi_api.merchant_details md ON md.merchant_id = m.id
  WHERE
    (
      m.business_banking = 1 --Inclduing merchants who are directly added on LMS
      OR ba_base.merchant_id IS NOT NULL
      OR oov.account_merchant_id__c IS NOT NULL
    )
    AND m._is_row_deleted IS NULL
),
features AS (
  SELECT
    entity_id,
    MAX(
      CASE
        WHEN name = 'skip_hold_funds_on_payout' THEN 1
      END
    ) AS is_funds_on_hold,
    MIN(
      CASE
        WHEN name = 'icici_2fa' THEN created_date
        ELSE NULL
      END
    ) AS icici_2fa_date,
    MIN(
      CASE
        WHEN name = 'payouts_on_hold' THEN created_date
        ELSE NULL
      END
    ) AS payouts_on_hold_date,
    MIN(
      CASE
        WHEN name = 'enable_smart_routing' THEN created_date
        ELSE NULL
      END
    ) AS mar_enabled_date
  FROM
    realtime_hudi_api.features
  WHERE
    entity_type = 'merchant'
    AND _is_row_deleted IS NULL
    AND name IN(
      'skip_hold_funds_on_payout',
      'icici_2fa',
      'payouts_on_hold',
      'enable_smart_routing'
    )
  GROUP BY
    1
),
merchant_users AS (
  SELECT
    merchant_id,
    try_cast(
      MIN(
        CASE
          WHEN product = 'primary' THEN FROM_UNIXTIME(created_at + 19800)
          ELSE NULL
        END
      ) AS timestamp
    ) AS pg_signup_at,
    try_cast(
      MIN(
        CASE
          WHEN product = 'banking' THEN FROM_UNIXTIME(created_at + 19800)
          ELSE NULL
        END
      ) AS timestamp
    ) AS x_signup_at
  FROM
    merchant_users
  GROUP BY
    merchant_id
)
