SELECT * FROM (
    SELECT	cs.cache_snapshot_id AS CacheSnapshotId,
            cs.subscription_data_set_id AS SubscriptionDataSetId,
            cs.run_id AS RunId,
            cs.entity_identifier AS EntityIdentifier,
            cs.entity_delta_code AS EntityDeltaCode,
            cs.entity_delta_date AS EntityDeltaDate,
            cs.entity_data_current AS EntityDataCurrent,
            cs.entity_data_previous AS EntityDataPrevious
    FROM dlta_cache_snapshot cs        
    WHERE cs.subscription_data_set_id = 88  AND cs.entity_identifier = 'CUR_HOUR' 
    ORDER BY cs.entity_delta_date DESC        
 ) WHERE ROWNUM = 1
FOR UPDATE
;

select * from dlta_cache_snapshot cs
WHERE cs.subscription_data_set_id = 88  AND cs.entity_identifier = 'CUR_HOUR' 
  AND run_id = (SELECT MIN(run_id) FROM dlta_cache_snapshot cs WHERE cs.subscription_data_set_id = 88  AND cs.entity_identifier = 'CUR_HOUR' )
---FOR UPDATE
;
rollback;
select max(entity_delta_code) over (rank() order by entity_delta_code) from dlta_cache_snapshot;

    SELECT TOP 1	cs.cache_snapshot_id AS CacheSnapshotId,
            cs.subscription_data_set_id AS SubscriptionDataSetId,
            cs.run_id AS RunId,
            cs.entity_identifier AS EntityIdentifier,
            cs.entity_delta_code AS EntityDeltaCode,
            cs.entity_delta_date AS EntityDeltaDate,
            cs.entity_data_current AS EntityDataCurrent,
            cs.entity_data_previous AS EntityDataPrevious
    FROM dlta_cache_snapshot cs        
    WHERE cs.subscription_data_set_id = 88  AND cs.entity_identifier = 'CUR_HOUR' 
        --AND cs.entity_delta_date = MAX(entity_delta_date)
    ORDER BY cs.entity_delta_date DESC        
    ;
  
rollback;