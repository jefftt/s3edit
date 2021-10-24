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
    println!(
        "renaming field: {} to {} in bucket: {} prefix: {}",
        params.source, params.target, s3url.bucket, s3url.prefix
    );
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

    let mut changed = false;
    let overwrite = jsonlines
        .map(|jsonline| {
            let mut json = jsonline.unwrap();
            // ignore error here, if the field doesn't exist then leave the JSON as is
            if let Ok(_) = rename(&mut json, &params.source, &params.target) {
                changed = true;
            }
            serde_json::to_vec(&json).unwrap()
        })
        .fold(Vec::new(), |mut acc, json| {
            acc.extend_from_slice(&json);
            acc.push(b'\n');
            acc
        });

    if !changed {
        println!("nothing to change, exiting");
        return Ok(());
    }

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

// Replaces the field name in place
fn rename(json: &mut Value, source: &str, target: &str) -> Result<()> {
    if let Some(root) = json.as_object_mut() {
        let mut path = json_pointer(source);
        let mut parent = root;
        loop {
            if path.is_empty() {
                break;
            } else {
                let head = path.pop().unwrap();
                if !parent.contains_key(&head) {
                    break;
                } else {
                    // found it
                    if path.is_empty() {
                        let value = parent.remove(&head).unwrap();
                        parent.insert(target.to_string(), value);
                        return Ok(());
                    } else {
                        // search the child object
                        if let Some(nested) = parent.get_mut(&head).unwrap().as_object_mut() {
                            parent = nested;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    return Err(anyhow!("field not found"));
}

fn json_pointer(s: &str) -> Vec<String> {
    if !s.starts_with('/') {
        return vec![s.to_string()];
    }
    s.split('/')
        .skip(1)
        .map(|x| x.replace("~1", "/").replace("~0", "~"))
        .collect()
}
