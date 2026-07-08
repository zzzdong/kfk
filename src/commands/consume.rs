use crate::cli::args::{ConsumeArgs, OffsetValue, OutputFormat};
use crate::cli::output;
use crate::client::{AdminClient, CliResult, create_consumer};
use std::time::Duration;

pub async fn handle_consume(args: ConsumeArgs, admin: AdminClient) {
    let result = consume_impl(args, admin).await;
    if let Err(e) = result {
        output::print_err(e);
    }
}

async fn consume_impl(args: ConsumeArgs, admin: AdminClient) -> CliResult<()> {
    let offset = match args.offset {
        OffsetValue::Earliest => kafka_client::AutoOffsetReset::Earliest,
        OffsetValue::Latest => kafka_client::AutoOffsetReset::Latest,
    };

    let client = admin.client();

    // Determine group mode vs direct (assign) mode
    let is_direct = args.group.eq_ignore_ascii_case("none");

    // Build group_id for group-mode consumption
    let group_id = if args.group.eq_ignore_ascii_case("random") {
        let g = format!("kfk-{}", uuid::Uuid::new_v4());
        output::print_note(format!("Using random group: {g}"));
        g
    } else if is_direct {
        // direct mode: no group, use assign instead of subscribe
        String::new()
    } else {
        args.group.clone()
    };

    let mut consumer = create_consumer(client, &group_id, offset).await?;

    if is_direct {
        // Direct mode: assign partitions manually
        let partitions = if let Some(p) = args.partition {
            vec![p]
        } else {
            // Get all partitions for the topic
            admin.refresh_metadata().await?;
            let metadata = client.metadata();
            let tm = metadata
                .get_topic(&args.topic)
                .await
                .ok_or_else(|| format!("Topic '{}' not found", args.topic))?;
            tm.partitions.iter().map(|p| p.partition_index).collect()
        };

        consumer
            .assign(&args.topic, partitions)
            .await
            .map_err(|e| format!("Failed to assign: {e}"))?;

        output::print_ok(format!(
            "Consuming from '{}' (direct assign, offset: {})",
            args.topic, args.offset
        ));
    } else {
        consumer
            .subscribe(vec![args.topic.clone()])
            .await
            .map_err(|e| format!("Failed to subscribe: {e}"))?;

        output::print_ok(format!(
            "Consuming from '{}' (group: {}, offset: {})",
            args.topic, group_id, args.offset
        ));
    }

    let pretty_output = args.output == OutputFormat::Pretty;
    let tail = args.tail;
    let mut consumed = 0;
    let mut empty_polls = 0u32;

    loop {
        match consumer.poll_timeout(Duration::from_millis(1000)).await {
            Ok(records) => {
                if records.is_empty() {
                    if tail.is_some() {
                        // --tail 模式：消费完后若持续 15 秒无数据则退出
                        if consumed > 0 {
                            empty_polls += 1;
                            if empty_polls >= 15 {
                                break;
                            }
                        }
                        continue;
                    }
                    // 无限模式：持续轮询，永不因无数据退出
                    continue;
                }

                for r in records {
                    if let Some(n) = tail
                        && consumed >= n
                    {
                        admin.close().await?;
                        return Ok(());
                    }

                    if pretty_output {
                        let record_json = serde_json::json!({
                            "topic": r.topic,
                            "partition": r.partition,
                            "offset": r.offset,
                            "timestamp": r.timestamp,
                            "key": r.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                            "payload": String::from_utf8_lossy(&r.value),
                            "headers": r.headers.iter().map(|h| {
                                serde_json::json!({
                                    "key": h.key,
                                    "value": String::from_utf8_lossy(&h.value)
                                })
                            }).collect::<Vec<_>>(),
                        });
                        let json_str = serde_json::to_string_pretty(&record_json)
                            .unwrap_or_else(|_| record_json.to_string());
                        match colored_json::to_colored_json(
                            &record_json,
                            colored_json::ColorMode::On,
                        ) {
                            Ok(colored) => println!("{colored}"),
                            Err(_) => println!("{json_str}"),
                        }
                    } else {
                        println!("{}", String::from_utf8_lossy(&r.value));
                    }
                    consumed += 1;
                }
            }
            Err(e) => {
                eprintln!("Poll error: {e}");
                break;
            }
        }
    }

    admin.close().await?;
    Ok(())
}
