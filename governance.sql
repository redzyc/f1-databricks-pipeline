
CREATE GROUP IF NOT EXISTS f1_analysts;
CREATE GROUP IF NOT EXISTS mclaren_engineering;

GRANT USAGE ON CATALOG formula1 TO f1_analysts;
GRANT USAGE ON SCHEMA formula1.gold TO f1_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA formula1.gold TO f1_analysts;


DENY SELECT ON SCHEMA formula1.bronze TO f1_analysts;

CREATE OR REPLACE FUNCTION formula1.gold.driver_email_mask(email STRING)
RETURN CASE
  WHEN is_account_group_member('f1_admins') THEN email
  ELSE 'REDACTED@f1.com'
END;

ALTER TABLE formula1.gold.drivers 
ALTER COLUMN driver_email SET MASK formula1.gold.driver_email_mask;


CREATE OR REPLACE FUNCTION formula1.gold.mclaren_row_filter(constructor_name STRING)
RETURN CASE
  WHEN is_account_group_member('f1_admins') THEN TRUE
  WHEN is_account_group_member('mclaren_engineering') AND constructor_name = 'McLaren' THEN TRUE
  ELSE FALSE
END;

ALTER TABLE formula1.gold.constructor_standings 
SET ROW FILTER formula1.gold.mclaren_row_filter ON (constructor_name);