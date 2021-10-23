use crate::{JsonFieldRenameParams, S3Url};
use anyhow::{anyhow, Result};
use aws_sdk_s3 as s3;
use futures::{stream, StreamExt};
use serde_json::Value;

pub(crate) async fn run(
    client: s3::Client,
    s3url: &S3Url,
    params: JsonFieldRenameParams,
) -> Result<()> {
    println!("{} {}", s3url.bucket, s3url.prefix);
    let mut page_token = None;
    let mut files = Vec::new();
    loop {
        let req = client
            .list_objects_v2()
            .bucket(&s3url.bucket)
            .prefix(&s3url.prefix)
            .set_continuation_token(page_token);
        let resp = req.send().await?;
        let keys: Vec<String> = resp
            .contents
            .unwrap_or_else(|| Vec::new())
            .iter()
            .filter_map(|md| md.key.as_ref().map(|s| s.clone()))
            .collect();
        files.extend_from_slice(&keys);
        if let Some(token) = resp.next_continuation_token {
            page_token = Some(token)
        } else {
            break;
        }
    }

    stream::iter(&files)
        .map(|file| process(&client, &s3url.bucket, &file, &params))
        .buffered(params.concurrency)
        .collect::<Vec<Result<()>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<()>>>()?;
    Ok(())
}

async fn process(
    client: &s3::Client,
    bucket: &str,
    file: &str,
    params: &JsonFieldRenameParams,
) -> Result<()> {
    let response = client.get_object().bucket(bucket).key(file).send().await?;
    let raw_body = &response.body.collect().await?.into_bytes();
    let jsonlines = serde_json::Deserializer::from_slice(&raw_body).into_iter::<Value>();

    let overwrite = jsonlines.fold(Vec::new(), |mut acc, json| {
        let renamed = rename(json.unwrap(), &params.source, &params.target).unwrap();
        acc.extend_from_slice(&serde_json::to_vec(&renamed).unwrap());
        acc.push(b'\n');
        acc
    });

    if params.dryrun {
        println!(
            "overwriting {} {} to:\n{}",
            bucket,
            file,
            String::from_utf8(overwrite).unwrap()
        );
    } else {
        client
            .put_object()
            .bucket(bucket)
            .key(file)
            .body(s3::ByteStream::from(overwrite))
            .send()
            .await?;
    }

    Ok(())
}

// TODO: support nested json
fn rename(mut json: Value, source: &str, target: &str) -> Result<Value> {
    if let Some(root) = json.as_object_mut() {
        if let Some(value) = root.remove(source) {
            root.insert(target.to_string(), value);
            return Ok(json);
        }
    }

    return Err(anyhow!("field not found"));
}
