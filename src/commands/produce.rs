use bytes::Bytes;
use kafka_client::{Header, ProducerRecord};

use crate::cli::args::ProduceArgs;
use crate::cli::output;
use crate::client::{AdminClient, CliResult};

pub async fn handle_produce(args: ProduceArgs, admin: AdminClient) {
    let result = produce_impl(args, admin).await;
    if let Err(e) = result {
        output::print_err(e);
    }
}

async fn produce_impl(args: ProduceArgs, admin: AdminClient) -> CliResult<()> {
    let producer = admin
        .create_producer()
        .await
        .map_err(|e| format!("Failed to create producer: {e}"))?;

    // Read from stdin
    let mut line = String::new();
    let mut count = 0;

    loop {
        line.clear();
        let bytes_read = std::io::stdin()
            .read_line(&mut line)
            .map_err(|e| format!("Failed to read stdin: {e}"))?;

        if bytes_read == 0 {
            break;
        }

        let value = line.trim_end_matches('\n').trim_end_matches('\r');

        if value.is_empty() {
            continue;
        }

        let mut record =
            ProducerRecord::new(&args.topic, Bytes::copy_from_slice(value.as_bytes()));

        if let Some(key) = &args.key {
            record = record.with_key(Bytes::copy_from_slice(key.as_bytes()));
        }

        if let Some(partition) = args.partition {
            record = record.with_partition(partition);
        }

        let headers: Vec<Header> = args
            .headers
            .iter()
            .map(|(k, v)| Header {
                key: k.clone(),
                value: Bytes::copy_from_slice(v.as_bytes()),
            })
            .collect();
        if !headers.is_empty() {
            record = record.with_headers(headers);
        }

        match producer.send(record).await {
            Ok(meta) => {
                count += 1;
                println!(
                    "Sent record to partition {} at offset {}.",
                    meta.partition, meta.offset
                );
            }
            Err(e) => {
                eprintln!("Failed to send record: {e}");
            }
        }
    }

    producer
        .flush()
        .await
        .map_err(|e| format!("Failed to flush: {e}"))?;

    if count > 0 {
        output::print_ok(format!("Sent {count} record(s) to '{}'", args.topic));
    } else {
        output::print_msg("No records sent (empty input).");
    }

    admin.close().await?;
    Ok(())
}
