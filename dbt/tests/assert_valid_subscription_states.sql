-- Custom dbt test: subscription current_status must be within accepted states
SELECT
    user_id,
    current_status
FROM {{ ref('int_subscription_history') }}
WHERE current_status NOT IN ('active', 'past_due', 'canceled', 'expired')
