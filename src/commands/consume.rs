use crate::cli::args::ConsumeArgs;
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
    let offset = match args.offset.to_lowercase().as_str() {
        "earliest" => kafka_client::AutoOffsetReset::Earliest,
        "latest" => kafka_client::AutoOffsetReset::Latest,
        _ => {
            output::print_err("Offset must be 'earliest' or 'latest'");
            kafka_client::AutoOffsetReset::Latest
        }
    };

    let client = admin.client();
    let mut consumer = create_consumer(client, &args.group, offset).await?;

    consumer
        .subscribe(vec![args.topic.clone()])
        .await
        .map_err(|e| format!("Failed to subscribe: {e}"))?;

    output::print_ok(format!(
        "Consuming from '{}' (group: {}, offset: {})",
        args.topic, args.group, args.offset
    ));

    // Wait for consumer group assignment
    tokio::time::sleep(Duration::from_secs(3)).await;

    let json_output = args.output == "json-each-row";
    let tail = args.tail.unwrap_or(usize::MAX);
    let mut consumed = 0;
    let mut empty_polls = 0;
    let max_empty_polls = if tail == usize::MAX { 60 } else { 15 };
    // If latest offset and no messages, give up after 15s
    let latest_timeout = args.offset == "latest";
    let max_empty_initial = if latest_timeout { 15 } else { 60 };

    loop {
        match consumer.poll(1000).await {
            Ok(records) => {
                if records.is_empty() {
                    empty_polls += 1;
                    if empty_polls >= max_empty_polls && consumed > 0 {
                        break;
                    }
                    if consumed == 0 && empty_polls >= max_empty_initial {
                        break; // No messages available
                    }
                    continue;
                } else {
                    empty_polls = 0;
                }

                for r in records {
                    if consumed >= tail {
                        admin.close().await?;
                        return Ok(());
                    }

                    if json_output {
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
                        println!("{record_json}");
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
