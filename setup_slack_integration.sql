-- ═══════════════════════════════════════════════════════════════
-- TREAD INTELLIGENCE: Slack Integration Setup
-- Run this after obtaining your Slack Incoming Webhook URL
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Create secret for Slack webhook (replace with your actual webhook secret)
-- The secret is the path after https://hooks.slack.com/services/
CREATE OR REPLACE SECRET TIRECO_DW.INTEGRATIONS.SLACK_WEBHOOK_SECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = '<YOUR_SLACK_WEBHOOK_SECRET_HERE>';

-- Step 2: Create Slack webhook notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION TIRECO_SLACK_WEBHOOK_INT
  TYPE = WEBHOOK
  ENABLED = TRUE
  WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET = TIRECO_DW.INTEGRATIONS.SLACK_WEBHOOK_SECRET
  WEBHOOK_BODY_TEMPLATE = '{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
  WEBHOOK_HEADERS = ('Content-Type'='application/json')
  COMMENT = 'Tread Intelligence Slack notifications for PO recommendations';

-- Step 3: Test Slack notification
CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
        SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('Test message from Tread Intelligence')
    ),
    SNOWFLAKE.NOTIFICATION.INTEGRATION('TIRECO_SLACK_WEBHOOK_INT')
);

-- Step 4: Resume notification tasks
ALTER TASK TIRECO_DW.ORCHESTRATION.TASK_DAILY_PO_NOTIFICATION RESUME;
ALTER TASK TIRECO_DW.ORCHESTRATION.TASK_WEEKLY_PO_NOTIFICATION RESUME;

-- Step 5: Test email notifications (replace with verified email)
-- CALL TIRECO_DW.ORCHESTRATION.SP_SEND_DAILY_EMAIL('your.email@company.com');
-- CALL TIRECO_DW.ORCHESTRATION.SP_SEND_WEEKLY_EMAIL('your.email@company.com');

-- ═══════════════════════════════════════════════════════════════
-- Verify setup
-- ═══════════════════════════════════════════════════════════════
SHOW NOTIFICATION INTEGRATIONS;
SHOW TASKS IN SCHEMA TIRECO_DW.ORCHESTRATION;
SELECT * FROM TIRECO_DW.ORCHESTRATION.NOTIFICATION_LOG ORDER BY SENT_AT DESC LIMIT 10;
