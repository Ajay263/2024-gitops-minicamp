select min from {{ source('training', 'numeric_column_anomalies_training') }} where min < 105
