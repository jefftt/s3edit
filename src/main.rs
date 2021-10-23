use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3 as s3;
use clap::{App, AppSettings, Arg, SubCommand};
use url::Url;

mod json_field_rename;

#[tokio::main]
async fn main() -> Result<()> {
    let opt = parse_args()?;

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = s3::Client::new(&shared_config);

    match opt.command {
        Command::JsonFieldRename(params) => {
            json_field_rename::run(client, &opt.s3url, params).await?
        }
    }
    Ok(())
}

fn parse_args() -> Result<Opt> {
    let matches = App::new("s3edit")
        .version("0.0.1")
        .about("Bulk edit s3 files")
        .arg(Arg::from_usage(
            "--url=<url> 's3 url to apply edits to recursively'",
        ))
        .setting(AppSettings::SubcommandRequired)
        .subcommand(
            SubCommand::with_name("json-field-rename")
                .about("rename json field for jsonline files")
                .arg(Arg::from_usage("--source <source> 'original field name, currently only supports flat JSON structure'"))
                .arg(Arg::from_usage("--target <target> 'new field name'"))
                .arg(Arg::from_usage(
                    "-p, --parallelism n 'max number of files to process at any given time, defaults to 5'",
                )),
        )
        .get_matches();

    let s3url = {
        let url = Url::parse(matches.value_of("url").unwrap()).unwrap();
        if url.scheme() != "s3" {
            panic!("unsupported url");
        }

        S3Url {
            bucket: url.host_str().unwrap().to_string(),
            prefix: url.path().trim_start_matches("/").to_string(),
        }
    };

    let command = {
        match matches.subcommand() {
            ("json-field-rename", Some(sub_m)) => Command::JsonFieldRename(JsonFieldRenameParams {
                source: sub_m.value_of("source").unwrap().to_string(),
                target: sub_m.value_of("target").unwrap().to_string(),
                concurrency: sub_m.value_of("parallel").map_or(5, |v| v.parse().unwrap()),
            }),
            (command, _) => return Err(anyhow!("Unknown command: {}", command)),
        }
    };

    Ok(Opt { s3url, command })
}

struct S3Url {
    bucket: String,
    prefix: String,
}

struct Opt {
    s3url: S3Url,
    command: Command,
}

enum Command {
    JsonFieldRename(JsonFieldRenameParams),
}

pub(crate) struct JsonFieldRenameParams {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) concurrency: usize,
}
