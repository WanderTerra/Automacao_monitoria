SELECT call_id, queue_id, start_time, answer_time, hangup_time, call_secs
FROM vonix.calls AS c
WHERE queue_id LIKE 'aguas%'
  AND queue_id NOT LIKE 'aguasguariroba%'
  AND status LIKE 'Completada%'
  AND start_time >= '2025-05-06 00:00:00'
  AND start_time < '2025-05-07 00:00:00'
  AND call_secs > 60
ORDER BY start_time DESC